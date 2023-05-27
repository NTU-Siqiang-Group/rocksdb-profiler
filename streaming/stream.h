#pragma once

#include <queue>
#include <iostream>
#include <mutex>

template<typename T>
class Channel {
  virtual void submit(const T& t) = 0;
  virtual T get() = 0;
  virtual void RegisterNext(Channel<T>* nxt) {}
  virtual bool is_empty() {
    return true;
  }
 public:
  std::queue<T>* data;
};

// not thread-safe
template<typename T>
class OutputChannel : public Channel<T> {
 public:
  virtual void submit(const T& t) override {
    for (auto& q : output_buffers) {
      q->push(t);
    }
  }
  T get() {}
  virtual void RegisterNext(Channel<T>* nxt) {
    if (nxt == nullptr || nxt->data == nullptr) {
      return;
    }
    output_buffers.push_back(nxt->data);
  }
  // output buffers
  std::vector<std::queue<T>*> output_buffers;
};
template<typename T>
class InputChannel : public Channel<T> {
 public:
  InputChannel() {
    this->data = new std::queue<T>();
  }
  ~InputChannel() {
    delete this->data;
  }
  void submit(const T& t) {};
  T get() override {
    T ret = this->data->front();
    this->data->pop();
    return ret;
  }
  bool is_empty() {
    return this->data->empty();
  }
};

class Computable {
 public:
  virtual void Compute() = 0;
};

template<typename T, typename C>
class Node : public Computable {
 public:
  Node()  {
    input_ = new InputChannel<T>();
    output_ = new OutputChannel<C>();
  }
  ~Node() {
    if (input_ != nullptr) {
      delete input_;
    }
    if (output_ != nullptr) {
      delete output_;
    }
  }
  // construct computation graph
  template<typename F>
  Node* Next(Node<C, F>* nxt) {
    assert(output_ != nullptr);
    output_->RegisterNext(nxt->input_);
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
  InputChannel<T>* input_;
  OutputChannel<C>* output_;
};

template <typename T>
class Source : public Node<T, T> {
 public:
  Source(): Node<T, T>() {
    delete this->input_;
    this->input_ = nullptr;
  }
  virtual void Start() = 0;
};

template<typename T>
class Sink : public Node<T, T> {
 public:
  Sink(): Node<T, T>() {
    delete this->output_;
    this->output_ = nullptr;
  }
  Node<T, T>* Next(Node<T, T>* nxt) = delete;
  void Emit(const T& t) = delete;
};