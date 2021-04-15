#ifndef DBTOASTER_MESSAGE_HPP
#define DBTOASTER_MESSAGE_HPP

#include <memory>
#include <vector>
#include <utility>

namespace dbtoaster {

struct MessageBase {
  virtual ~MessageBase() = default;
};

template <class T>
struct Message : MessageBase {
  T content;
};

template <class Key, class Value>
struct BatchMessage : MessageBase {
  using KVpair = std::pair<Key, Value>;
  std::vector<KVpair> content;

  void append(const Key& key, const Value& value) {
    content.push_back(std::make_pair(key, value));
  }
};

typedef std::unique_ptr<MessageBase> MessageBasePtr;

}

#endif /* DBTOASTER_MESSAGE_HPP */