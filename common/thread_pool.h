namespace kvs {

#include <condition_variable>
#include <function>
#include <mutex>
#include <queue>
#include <vector>

template<typename F, typename... Args>
class ThreadPool {
  public:
    ThreadPool() = default

    ~ThreadPool;

    // Copy assignment/constructor
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Move assignment/constructor
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    // Overload operator()
    void operator()() {

    }

    void Enqueue(Functor&& functor, Args&& ...args);

  private:
    std::condition_variable cv_;

    std::mutex mutex_;

    std::queue tasks_;

    std::vector<std::thread> pools_;
};

template<typename F, typename... Args>
void ThreadPool::Enqueue(Functor&& functor, Args&& ...args) {

}

} // namespace kvs