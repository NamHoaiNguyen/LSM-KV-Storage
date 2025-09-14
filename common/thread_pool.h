#ifndef COMMON_THREAD_POOL_H
#define COMMON_THREAD_POOL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <vector>

namespace {

constexpr unsigned int kDefaultNumThreadsInThreadPool = 8;

} // namespace

namespace kvs {

class ThreadPool {
public:
  inline ThreadPool();

  inline ~ThreadPool();

  // No copy allowed
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  // No move allowed
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  template <typename Functor, typename... Args>
  inline decltype(auto) Enqueue(Functor &&functor, Args &&...args);

private:
  std::condition_variable cv_;

  std::mutex mutex_;

  unsigned int num_threads_;

  std::atomic<bool> shutdown_;

  std::queue<std::function<void()>> jobs_;

  std::vector<std::thread> workers_;
};

ThreadPool::ThreadPool() : shutdown_(false) {
  unsigned int num_threads_ = std::thread::hardware_concurrency();
  if (num_threads_ == 0) {
    num_threads_ = kDefaultNumThreadsInThreadPool;
  }

  for (int i = 0; i < num_threads_; i++) {
    workers_.emplace_back([this]() {
      while (!shutdown_) {
        std::function<void()> job;
        {
          std::unique_lock lock(mutex_);
          cv_.wait(lock, [this]() {
            return this->shutdown_ || !this->jobs_.empty();
          });

          if (this->shutdown_ && this->jobs_.empty()) {
            return;
          }

          job = std::move(jobs_.front());
          jobs_.pop();
        }
        job();
      }
    });
  }
}

ThreadPool::~ThreadPool() {
  shutdown_ = true;

  // Finish remaining jobs
  cv_.notify_all();

  for (int i = 0; i < num_threads_; i++) {
    workers_[i].join();
  }
}

template <typename Functor, typename... Args>
decltype(auto) ThreadPool::Enqueue(Functor &&functor, Args &&...args) {
  using ReturnType = std::invoke_result_t<Functor, Args...>;
  auto task = std::make_shared<std::packaged_task<ReturnType()>>(
      std::bind(std::forward<Functor>(functor), std::forward<Args>(args)...));
  std::future<ReturnType> result = task->get_future();
  std::function<void()> job = [task]() { return (*task)(); };

  if (shutdown_.load()) {
    throw std::runtime_error(
        "Couldn't ask new task because threadpool was shut down");
  }

  {
    std::scoped_lock lock(mutex_);
    jobs_.emplace(std::move(job));
  }

  cv_.notify_one();
  return result;
}

} // namespace kvs

#endif // COMMON_THREAD_POOL_H