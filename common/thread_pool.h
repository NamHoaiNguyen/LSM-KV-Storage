namespace kvs {

#include <condition_variable>
#include <functional>
#include <mutex>
#include <packaged_task>
#include <queue>
#include <vector>

template <typename F, typename... Args> class ThreadPool {
public:
  ThreadPool() = default

      ~ThreadPool;

  // Copy assignment/constructor
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  // Move assignment/constructor
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  // Overload operator()
  void operator()() {}

  void Enqueue(Functor &&functor, Args &&...args);

private:
  std::condition_variable cv_;

  std::mutex mutex_;

  std::queue<std::function<void>()> jobs_;

  std::vector<std::thread> pools_;
};

template <typename F, typename... Args>
void ThreadPool::Enqueue(Functor &&functor, Args &&...args) {
  using ReturnType = std::invoke_result_t<Functor, Args...>;
  auto task = std::make_shared<std::packaged_task<ReturnType>>(
      std::bind(std::forward<Functor>(functor), std::forward<Args>(args)...));
  std::future<ReturnType> result = task->get_future();
  std::function<void()> job = [task]() { return (*task)(); }

  {
    std::scoped_lock<std::mute> lock(mutex_);
    jobs_.emplace(std::move(job));
  }
}

} // namespace kvs