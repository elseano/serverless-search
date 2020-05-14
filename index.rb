require 'digest/md5'
require 'aws-sdk'

require './worker'

Aws.config = {:access_key_id => 'KEY', :secret_access_key => 'SECRET', region: "BLAH"}
DYNAMO = Aws::DynamoDB::Client.new(endpoint: 'http://localhost:8000')
TABLE_NAME = "Index"


def import_all
    file = File.new("test_corpus/Wikipedia-20190326100926.xml" )
    doc = REXML::Document.new file

    capacity = 0

    REXML::XPath.each(doc, "/mediawiki/page") do |page|
        page_id = page.get_text("id").value
        $stdout.write "Indexing #{page_id}... "
        REXML::XPath.each(page, "revision/text") do |text|
            cleaned = clean_wiki(text.get_text.value)

            begin
                result = index_doc(page_id, strip_html(cleaned))
                $stdout.write result
                capacity = capacity + result[:consumed_capacity]
                puts " done."
            rescue => ex
                puts "Error in document #{page_id}"
            end
        end
    end

    puts "Documents indexed. Capacity used: #{capacity}"
end

def get_doc_original(id)
    file = File.new("test_corpus/Wikipedia-20190326100926.xml" )
    doc = REXML::Document.new file

    capacity = 0

    REXML::XPath.each(doc, "/mediawiki/page") do |page|
        page_id = page.get_text("id").value
        REXML::XPath.each(page, "revision/text") do |text|
            return text.get_text.value if page_id.to_s == id.to_s
        end
    end
    return nil
end

def strip_html(value)
    value = value.gsub("<br>", "<br/>")
    doc = REXML::Document.new "<root>#{value}</root>"
    doc.root.texts.each(&:value).join(" ")
end


def clean_wiki(input)
    input.gsub(/\{\{.*?\}\}/m, "").gsub(/\[\[File\:.*?\]\]/, "").gsub(/\[\[.*?\|(.*?)\]\]/, '\1').gsub(/\[\[(.*?)\]\]/, '\1').gsub(/\=+(.*?)\=+/, '\1').gsub(/\[http[s]?:\/\/.*?[^\w]\"(.*?)\"\]/, '\1')
end

def setup
    puts "Creating table..."

    DYNAMO.create_table({
        attribute_definitions: [
            {
                attribute_name: "PK", 
                attribute_type: "S", 
            }, 
            {
                attribute_name: "SK", 
                attribute_type: "S", 
            }, 
        ],
        key_schema: [
            {
                attribute_name: "PK", 
                key_type: "HASH", 
            }, 
            {
                attribute_name: "SK", 
                key_type: "RANGE", 
            }, 
        ], 
        provisioned_throughput: {
            read_capacity_units: 5, 
            write_capacity_units: 5, 
        }, 
        table_name: TABLE_NAME
    })

    loop do
        sleep(1)
        status = DYNAMO.describe_table({
            table_name: TABLE_NAME, 
          })[:table][:table_status]

        puts status

        break if status == "ACTIVE"
    end
end

def search(text)
    words = text.downcase.split(/\s+/).map { |w| get_word(stem(w)) }

    total_docs = 10 # TODO - Good way to get this.
    ranked_docs = Hash.new do 
        { score: 0, words: [] }
    end

    words.each do |word_result|
        word_result[:docs].each do |doc|
            doc_id = doc.keys[0]
            tf = doc.values[0]

            idf = Math.log(total_docs / (tf + 1)) 
            tf_idf = tf * idf 
            
            rank_rec = ranked_docs[doc_id]
            rank_rec[:score] = (rank_rec[:score] + tf_idf).to_f
            rank_rec[:words] << { word: word_result[:word], tf: tf.to_f, tf_idf: idf.to_f }
            ranked_docs[doc_id] = rank_rec
        end
    end

    ranked_docs.to_a.sort_by do |(doc_id, rank_rec)|
        rank_rec[:score] * -1
    end
end

def index_doc(id, text)
    index = construct_index(text)
    put(index, id)
end

def get_word(word)
    pk = make_pk(word)
    sk = "#{word}-"

    items = DYNAMO.query({
        table_name: TABLE_NAME,
        key_condition_expression: "PK = :pk AND begins_with(SK, :sk)",
        expression_attribute_values: {
            ":pk" => pk, ":sk" => sk
        }
    })

    result = []
    tf = nil

    items.items.each do |item|
        if item["SK"] == "#{sk}TF"
            tf = item["freq"]
        else
            result << { item["SK"].sub(sk, "") => item["freq"] }
        end
    end

    { tf: tf, docs: result, word: word }
end

def show_index
    DYNAMO.scan({ table_name: TABLE_NAME }).items.each do |item|
        puts item
    end
end

def construct_index(data)
    words = data.downcase.split(/\s+/)
    words.reduce(Hash.new(0)) { |acc, word| acc[word] = acc[word] + 1; acc }
end

def put(index, document_id)
    ops = index.reduce([]) do |commands, (word, freq)|
        pk = make_pk(word)
        sk = make_sk(word, document_id)
        meta = "#{word}-TF"
        commands << ["PUT", pk, sk, freq]
        commands << ["INC", pk, meta, freq]
    end

    worker = Worker.new(threads: 5)

    ops.each do |(op, pk, sk, data)|
        worker.push_work(proc { execute_op(op, pk, sk, data) })        
    end

    results = worker.execute

    {
        consumed_capacity: results[:results].inject(&:+),
        time: results[:time],
        unique_words: ops.length / 2,
        total_words: index.values.inject(&:+)
    }
    
end

def execute_op(op, pk, sk, data)
    case op
    when "PUT"
        resp = DYNAMO.put_item({
            item: {
                "PK" => pk,
                "SK" => sk,
                "freq" => data
            },
            return_consumed_capacity: "TOTAL",
            table_name: TABLE_NAME
        })

        return resp.consumed_capacity.capacity_units
    when "INC"
        params = {
            table_name: TABLE_NAME,
            key: { "PK" => pk, "SK" => sk },
            update_expression: "ADD freq :val",
            expression_attribute_values: {
                ':val' => data,
            },
            return_consumed_capacity: "TOTAL",
        }

        resp = DYNAMO.update_item(params)
        return resp.consumed_capacity.capacity_units
    end
end

def stem(word)
    word
    .sub(/ing$/, "")
    .sub(/s$/, "")
    .sub(/ed$/, "e")

end

def make_pk(word)
    Digest::MD5.hexdigest(stem(word))
end

def make_sk(word, document_id)
    "#{stem(word)}-#{document_id}"
end