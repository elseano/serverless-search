require 'rexml/document'

file = File.new("test_corpus/Wikipedia-20190326100926.xml" )
doc = REXML::Document.new file

REXML::XPath.each(doc, "/mediawiki/page/revision/text") do |text|
    puts text.get_text.value.length
end