#ifndef DB_BASE_ITERATOR_H
#define DB_BASE_ITERATOR_H

namespace kvs {

class BaseIterator {
  virtual ~BaseIterator();

  virtual bool operator==(const BaseIterator &other) = 0;

  virtual bool operator!=(const BaseIterator &other) = 0;

  virtual BaseIterator &operator++() = 0;
};

} // namespace kvs

#endif // DB_BASE_ITERATOR_H