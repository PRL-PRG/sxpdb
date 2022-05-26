#ifndef SXPDB_STABLE_VECTOR_H
#define SXPDB_STABLE_VECTOR_H

#include <cstddef>
#include <vector>
#include <stdexcept>
#include <unistd.h>
#include <iterator>
#include <cassert>

template<typename T>
class StableVectorIterator;

template<typename T>
class ConstStableVectorIterator;

/**
 * Defines a stable vector, i.e. addresses won't change even in case of
 * reallocation.
 * The price is contiguity, but we make it more flexible and exposes more internals
 * than a deque for better performance.
 * It allow only constant insertion at the end, not at the front.
 */
template<typename T>
class StableVector {
  friend class StableVectorIterator<T>;
  friend class ConstStableVectorIterator<T>;
private:
  std::vector<std::vector<T>> data;
  size_t total_size = 0;
  size_t elem_capacity = 0;
  size_t last_chunk = 0;
  size_t nb_pages = 1; // how many multiple of the page size to allocate
  size_t page_size;

  // returns true if it created a new chunk
  bool pre_reserve(size_t new_cap) {
    if(new_cap > elem_capacity) {
      size_t chunk_size = new_cap - elem_capacity;
      if(data.size() > 0 && data[last_chunk].size() == 0) {
        // the last chunk is empty so we can directly extend it instead of
        // allocating a new chunk
        size_t old_chunk_capacity = data[last_chunk].capacity();
        data[last_chunk].reserve(chunk_size + data[last_chunk].capacity());
        elem_capacity += data.back().capacity() - old_chunk_capacity;
        return false;
      }
      else {
        std::vector<T> chunk;
        data.push_back(std::vector<T>());
        data.back().reserve(chunk_size);
        elem_capacity += data.back().capacity();
        return true;
      }
    }
    return false;
  }

public:
  using value_type = T;

  StableVector() : page_size(sysconf(_SC_PAGE_SIZE)) {
    // reserve a page worth of elements
    reserve(page_size / sizeof(T));
  }

  StableVector(size_t count, const T& value = T()) : total_size(count), page_size(sysconf(_SC_PAGE_SIZE)) {
    // round up count at a multiple of a page size?
    data.push_back(std::vector(count, value));
    elem_capacity = data[0].capacity();
  }

  // Just reserve
  StableVector(size_t count) : page_size(sysconf(_SC_PAGE_SIZE)) {
    std::vector<T> chunk;
    // round count to a multiple of a page size?
    data.push_back(chunk);
    data[0].reserve(count);
    elem_capacity = data[0].capacity();
  }

  size_t size() const { return total_size; }

  size_t capacity() const { return capacity; }

  size_t nb_chunks() const {return data.size(); }

  void reserve(size_t new_cap) {
    pre_reserve(new_cap);
  }

  T& at(size_t pos) {
    size_t p = pos;
    for(auto& chunk : data) {
      if(p < chunk.size()) {
        return chunk[p];
      }
      p -= chunk.size();
    }
    throw std::out_of_range("Out of range access in stable vector.\n");
  }

  const T& at(size_t pos) const {
    size_t p = pos;
    for(auto& chunk : data) {
      if(p < chunk.size()) {
        return chunk[p];
      }
      p -= chunk.size();
    }
    throw std::out_of_range("Out of range access in stable vector.\n");
  }

  T& operator[](size_t pos) {
    size_t p = pos;
    for(auto& chunk : data) {
      if(p < chunk.size()) {
        return chunk[p];
      }
      p -= chunk.size();
    }
    //that will access out of bound memory
    return (data.back())[p];
  }


  const T& operator[](size_t pos) const {
    size_t p = pos;
    for(auto& chunk : data) {
      if(p < chunk.size()) {
        return chunk[p];
      }
      p -= chunk.size();
    }
    //that will access out of bound memory
    return (data.back())[p];
  }

  T& back() {
    return data[last_chunk].back();
  }

  const T& back() const {
    return data[last_chunk].back();
  }

  void push_back(const T& value) {
    // do we need to allocate a new chunk?
    // we need last_chunk because people could have called reserve although there was
    // still some capacity in the last chunk
    if(data.size() > 0 && data[last_chunk].capacity() - data[last_chunk].size() > 0) {
      data[last_chunk].push_back(value);
    }
    else {
      //allocate a new chunk with double the size of the previous one
      //
      bool res = pre_reserve(elem_capacity + nb_pages * page_size / sizeof(T));
      if(res) {last_chunk++;}
      nb_pages *= 2;
      data[last_chunk].push_back(value);
    }
    total_size++;
  }

  void push_back(const T&& value) {
    if(data[last_chunk].capacity() - data[last_chunk].size() > 0) {
      data[last_chunk].push_back(value);
    }
    else {
      bool res = pre_reserve(elem_capacity + nb_pages * page_size / sizeof(T));
      if(res) {last_chunk++;}
      nb_pages *= 2;
      data[last_chunk].push_back(value);
    }
    total_size++;
  }

  void resize(size_t count) {
    size_t to_add = count;
    // is there capacity in the last chunk?
    if(data.size() > 0 && data[last_chunk].capacity() > data[last_chunk].size()) {
      data[0].resize(std::min(count, data[0].capacity()));
      to_add -= data[last_chunk].size();
    }
    if(to_add > 0) {
      data.push_back(std::vector<T>(to_add));
    }
    elem_capacity = std::max(elem_capacity, count);
    total_size = count;
  }

  template< class InputIt>
  StableVectorIterator<T> insert(StableVectorIterator<T> pos, InputIt first, InputIt last ) {
    size_t length = last - first;
    bool res = pre_reserve(total_size + length);

    // insert in the remaining of the last chunk
    size_t available_size_chunk = data[last_chunk].capacity() - data[last_chunk].size();

    StableVectorIterator ret(*this, last_chunk, data[last_chunk].size() - 1);

    if(available_size_chunk > 0) {
      if(length < available_size_chunk) {
        data[last_chunk].insert(data[last_chunk].end(), first, last);
        return ret;
      }
      else {
        data[last_chunk].insert(data[last_chunk].end(), first, first + available_size_chunk);
      }
    }
    else {
      ret.chunk_pos = last_chunk + 1;
      ret.pos = 0;
    }

    assert(!res || available_size_chunk < length);

    last_chunk++;
    data[last_chunk].insert(data[last_chunk].end(), first + available_size_chunk, last);

    total_size += length;

    elem_capacity = std::max(elem_capacity, total_size);

    return ret;
  }

  void clear() {
    for(auto& chunk : data) {
      chunk.clear();
    }
    last_chunk = 0;
    total_size = 0;
  }

  bool empty() const noexcept {
    return total_size == 0;
  }

  StableVectorIterator<T> begin() { return StableVectorIterator<T>(*this); }
  StableVectorIterator<T> end() { return StableVectorIterator<T>(*this, total_size > 0 ? data.size() : 0, 0); }
  ConstStableVectorIterator<T> begin() const { return ConstStableVectorIterator<T>(*this); }
  ConstStableVectorIterator<T> end() const { return ConstStableVectorIterator<T>(*this, total_size > 0 ? data.size() : 0, 0); }

  ConstStableVectorIterator<T> cbegin() const { return ConstStableVectorIterator<T>(*this); }
  ConstStableVectorIterator<T> cend() const { return ConstStableVectorIterator<T>(*this, total_size > 0 ? data.size() : 0, 0); }

  const std::vector<T>& chunk(size_t pos) const {
    return data.at(pos);
  }


  virtual ~StableVector() {  }
};

template<typename T>
class StableVectorIterator {
  friend class StableVector<T>;
public:
  using iterator_category = std::random_access_iterator_tag;
  using difference_type   = std::ptrdiff_t;
  using value_type        = T;
  using pointer           = T*;  // or also value_type*
  using reference         = T&;  // or also value_type&

  reference operator*() const { return v.data[chunk_pos][pos];}
  pointer operator->() { return &v.datadata[chunk_pos][pos]; }
  reference operator[](size_t i) { return v[i]; }


  StableVectorIterator& operator++() {
    if(pos < v.data[chunk_pos].size() - 1) {
      pos++;
      assert(pos > 0);
    }
    else {
      chunk_pos++;
      pos = 0;
      assert(chunk_pos > 0);
    }
    return *this;
  }

  StableVectorIterator operator++(int) {
    StableVectorIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  StableVectorIterator& operator+=(difference_type rhs) {
    difference_type shift = rhs;
    int sign  = rhs > 0 ? 1 : -1;
    while(pos + shift >= v.data[chunk_pos].size() || pos < 0) {
      shift -= v.data[chunk_pos].size();
      pos += shift;
      chunk_pos += sign;
      assert(pos >= 0);
      assert(chunk_pos >= 0);
    }

    return *this;
  }

  StableVectorIterator operator+(difference_type rhs) const {
    StableVectorIterator tmp = *this;
    tmp += rhs;
    return tmp;
  }

  StableVectorIterator& operator-=(difference_type rhs) {
    return (*this) += - rhs;
  }



  StableVectorIterator& operator--() {
    if(pos > 0 ) {
      pos--;
    }
    else {
      chunk_pos--;
      assert(chunk_pos > 0);
      pos = v.data[chunk_pos].size() - 1;
    }
    return *this;
  }

  StableVectorIterator operator--(int) {
    StableVectorIterator tmp = *this;
    --(*this);
    return tmp;
  }

  StableVectorIterator operator-(difference_type rhs) const {
    StableVectorIterator tmp = *this;
    tmp -= rhs;
    return tmp;
  }


  difference_type operator-(const StableVectorIterator& rhs) const {
    ptrdiff_t diff = 0;
    size_t chunk_pos1 = chunk_pos;
    size_t chunk_pos2 = rhs.chunk_pos;
    size_t pos1 = pos;
    size_t pos2 = rhs.pos;
    int sign = 1;
    if(chunk_pos < rhs.chunk_pos || (chunk_pos== rhs.chunk_pos && pos < rhs.pos)) {
        chunk_pos1 = rhs.chunk_pos;
        chunk_pos2 = chunk_pos;

        pos1 = rhs.pos;
        pos2 = pos;

        sign = -1;
    }
    for(size_t i = chunk_pos1; i < chunk_pos2 ; i++) {
      diff += v.data[i].size();
    }
    diff += pos2 - pos1;
    assert(diff > 0);
    return sign * diff;
  }

  friend inline StableVectorIterator operator+(difference_type lhs, const StableVectorIterator& rhs) {
    StableVectorIterator tmp = rhs;
    tmp += lhs;
    return tmp;
  }

  friend inline StableVectorIterator operator-(difference_type lhs, const StableVectorIterator& rhs) {
    StableVectorIterator tmp = rhs;
    tmp -= lhs;
    return tmp;
  }

  friend bool operator== (const StableVectorIterator& a, const StableVectorIterator& b) {
    return a.chunk_pos == b.chunk_pos && a.pos == b.pos;
  }
  friend bool operator!= (const StableVectorIterator& a, const StableVectorIterator& b) { return !(a == b); }
  friend bool operator>(const StableVectorIterator& a, const StableVectorIterator& b) {
    return a.chunk_pos > b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos > b.pos);
  }
  friend bool operator<(const StableVectorIterator& a, const StableVectorIterator& b) {
    return a.chunk_pos < b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos < b.pos);
  }
  friend bool operator>=(const StableVectorIterator& a, const StableVectorIterator& b) {
    return a.chunk_pos > b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos >= b.pos);
  }
  friend bool operator<=(const StableVectorIterator& a, const StableVectorIterator& b) {
    return a.chunk_pos < b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos <= b.pos);
  }

private:
  StableVectorIterator(StableVector<T>& vec, size_t chunk_pos_= 0 , size_t pos_ = 0) :
  v(vec), chunk_pos(chunk_pos_), pos(pos_) {}

  StableVector<T>& v;
  size_t chunk_pos = 0;
  size_t pos = 0;
};

template<typename T>
class ConstStableVectorIterator {
  friend class StableVector<T>;
public:
  using iterator_category = std::random_access_iterator_tag;
  using difference_type   = std::ptrdiff_t;
  using value_type        = T;
  using pointer           = T*;  // or also value_type*
  using reference         = T&;  // or also value_type&
  using const_reference   = const T&;

  const_reference operator*() const {
   return v.data[chunk_pos][pos];
  }
  const pointer operator->() { return &v.get(chunk_pos, pos); }
  const_reference operator[](size_t i) { return v[i]; }


  ConstStableVectorIterator& operator++() {
    if(pos < v.data[chunk_pos].size() - 1) {
      pos++;
    }
    else {
      chunk_pos++;
      pos = 0;
    }
    return *this;
  }

  ConstStableVectorIterator operator++(int) {
    ConstStableVectorIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  ConstStableVectorIterator& operator+=(difference_type rhs) {
    difference_type shift = rhs;
    pos+= shift;
    int sign  = rhs > 0 ? 1 : -1;
    while(pos >= v.data[chunk_pos].size() || pos < 0) {
      shift -= v.data[chunk_pos].size();
      pos += shift;
      chunk_pos += sign;
      assert(pos >= 0);
      assert(chunk_pos >= 0);
    }

    return *this;
  }

  ConstStableVectorIterator operator+(difference_type rhs) const {
    ConstStableVectorIterator tmp = *this;
    tmp += rhs;
    return tmp;
  }

  ConstStableVectorIterator& operator-=(difference_type rhs) {
    return (*this) += - rhs;
  }



  ConstStableVectorIterator& operator--() {
    if(pos > 0 ) {
      pos--;
    }
    else {
      chunk_pos--;
      assert(chunk_pos > 0);
      pos = v.data[chunk_pos].size() - 1;
    }
    return *this;
  }

  ConstStableVectorIterator operator--(int) {
    ConstStableVectorIterator tmp = *this;
    --(*this);
    return tmp;
  }

  ConstStableVectorIterator operator-(difference_type rhs) const {
    ConstStableVectorIterator tmp = *this;
    tmp -= rhs;
    return tmp;
  }


  difference_type operator-(const ConstStableVectorIterator& rhs) const {
    ptrdiff_t diff = 0;
    size_t chunk_pos1 = chunk_pos;
    size_t chunk_pos2 = rhs.chunk_pos;
    size_t pos1 = pos;
    size_t pos2 = rhs.pos;
    int sign = 1;
    if(chunk_pos < rhs.chunk_pos || (chunk_pos== rhs.chunk_pos && pos < rhs.pos)) {
      chunk_pos1 = rhs.chunk_pos;
      chunk_pos2 = chunk_pos;

      pos1 = rhs.pos;
      pos2 = pos;

      sign = -1;
    }
    for(size_t i = chunk_pos1; i < chunk_pos2 ; i++) {
      diff += v.data[i].size();
    }
    diff += pos2 - pos1;
    assert(diff > 0);
    return sign * diff;
  }

  friend inline ConstStableVectorIterator operator+(difference_type lhs, const ConstStableVectorIterator& rhs) {
    ConstStableVectorIterator tmp = rhs;
    tmp += lhs;
    return tmp;
  }

  friend inline ConstStableVectorIterator operator-(difference_type lhs, const ConstStableVectorIterator& rhs) {
    ConstStableVectorIterator tmp = rhs;
    tmp -= lhs;
    return tmp;
  }

  friend bool operator== (const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) {
    return a.chunk_pos == b.chunk_pos && a.pos == b.pos;
  }
  friend bool operator!= (const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) { return !(a == b); }
  friend bool operator>(const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) {
    return a.chunk_pos > b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos > b.pos);
  }
  friend bool operator<(const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) {
    return a.chunk_pos < b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos < b.pos);
  }
  friend bool operator>=(const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) {
    return a.chunk_pos > b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos >= b.pos);
  }
  friend bool operator<=(const ConstStableVectorIterator& a, const ConstStableVectorIterator& b) {
    return a.chunk_pos < b.chunk_pos || (a.chunk_pos == b.chunk_pos && a.pos <= b.pos);
  }

private:
  ConstStableVectorIterator(const StableVector<T>& vec, size_t chunk_pos_= 0 , size_t pos_ = 0) :
  v(vec), chunk_pos(chunk_pos_), pos(pos_) {}

  const StableVector<T>& v;
  size_t chunk_pos = 0;
  size_t pos = 0;
};

#endif
