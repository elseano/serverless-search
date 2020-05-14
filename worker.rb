class Worker
    def initialize(threads:)
        @thread_count = threads
        reset
    end

    def reset
        @work = Queue.new
        @results = Queue.new
        @threads = []
    end

    def push_work(proc)
        @work << proc
    end

    def execute()    
        start = Time.now.to_i    
        spawn_threads
        @work.close
        results = []

        loop do
            this_result = @results.pop
            if this_result == "END" 
                stop_threads
                break
            end
            results << this_result
        end

        ended = Time.now.to_i

        { :time => ended - start, results: results }
    end

    def stop_threads
        @threads.each { |t| t.exit }
        reset
    end

    private

    def spawn_threads
        @thread_count.times do
            @threads << Thread.new do
                while e = @work.deq
                    result = e.call
                    @results << result
                end

                @results << "END"
            end
        end
    end
    
end


def test_worker
    w = Worker.new
    w.push_work(proc { 1 })
    w.push_work(proc { 2 })
    w.push_work(proc { 3 })
    w.push_work(proc { 4 })
    w.push_work(proc { 5 })
    w.push_work(proc { 6 })
    w.push_work(proc { 7 })
    w.execute
end


def test_worker_2
    w = Worker.new
    w.push_work(proc { 1 })
    w.execute
end
