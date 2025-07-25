namespace kvs {

#include <condition_variable>
#include <functional>
#include <mutex>
#include <packaged_task>
#include <queue>
#include <vector>

class ThreadPool {
public:
  ThreadPool() = default

  ~ThreadPool() = default;

  // Copy assignment/constructor
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  // Move assignment/constructor
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  // Overload operator()
  void operator()() {}

  template <typename Functor, typename... Args>
  void Enqueue(Functor &&functor, Args &&...args);

private:
  std::condition_variable cv_;

  std::mutex mutex_;

  std::atomic<bool> shutdown_;

  std::queue<std::function<void>()> jobs_;

  std::vector<std::thread> pools_;
};

template <typename Functor, typename... Args>
void ThreadPool::Enqueue(Functor &&functor, Args &&...args) {
  using ReturnType = std::invoke_result_t<Functor, Args...>;
  auto task = std::make_shared<std::packaged_task<ReturnType>>(
      std::bind(std::forward<Functor>(functor), std::forward<Args>(args)...));
  std::future<ReturnType> result = task->get_future();
  std::function<void()> job = [task]() { return (*task)(); }

  if (shutdown_.load()) {
    std::throw_error("Couldn't ask new task because threadpool was shut down");
  }

  {
    std::scoped_lock<std::mute> lock(mutex_);
    jobs_.emplace(std::move(job));
  }
}

} // namespace kvs