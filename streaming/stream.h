#pragma once

#include <queue>
#include <iostream>
#include <mutex>

// not thread-safe
template<typename T>
struct Channel {
  Channel() {
    data = new std::queue<T>();
  }
  void submit(const T& t) {
    data->push(t);
  }
  T get() {
    T t = data->front();
    data->pop();
    return t;
  }
  bool is_empty() {
    return data->empty();
  }
  ~Channel() {
    if (!from_output) {
      delete data;
    }
  }
  std::queue<T>* data;
  bool from_output = false;
};

class Computable {
 public:
  virtual void Compute() = 0;
};

template<typename T, typename C>
class Node : public Computable {
 public:
  Node(Channel<T>* input = nullptr, Channel<C>* output=nullptr): next_(nullptr), input_(input), output_(output) {
    if (input == nullptr) {
      input_ = new Channel<T>();
    }
    if (output == nullptr) {
      output_ = new Channel<C>();
    }
  }
  ~Node() {
    delete input_;
    delete output_;
  }
  // construct computation graph
  template<typename F>
  Node* Next(Node<C, F>* nxt) {
    assert(output_ != nullptr);
    delete nxt->input_->data;
    nxt->input_->data = output_->data;
    nxt->input_->from_output = true;
    next_ = nxt;
    return this;
  }
  std::vector<T> Recv() {
    assert(input_ != nullptr);
    if (input_->is_empty()) {
      return {};
    }
    std::vector<T> ret;
    while (!input_->is_empty()) {
      ret.push_back(input_->get());
    }
    return ret;
  }
  void Emit(const C& c) {
    assert(output_ != nullptr);
    output_->submit(c);
  }
  void Compute() final {
    ComputeImpl();
    if (next_ == nullptr) {
      return;
    }
    next_->Compute(); // invoke child's Compute()
  }
  virtual void ComputeImpl() = 0;
  Computable* next_;
  Channel<T>* input_;
  Channel<C>* output_;
};

template <typename T>
class Source : public Node<T, T> {
 public:
  Source(): Node<T, T>() {}
  virtual void Start() = 0;
};

template<typename T>
class Sink : public Node<T, T> {
 public:
  Sink(): Node<T, T>() {}
  Node<T, T>* Next(Node<T, T>* nxt) = delete;
  void Emit(const T& t) = delete;
};