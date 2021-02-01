import io


def sub_read(fragment, iter, n):
    """fragment is a string of length < n """
    try:
        next_ = next(iter)
    except StopIteration:
        return fragment, ''
    else:
        result = fragment + next_
        if len(result) >= n:
            return result[:n], result[n:]
        else:
            return sub_read(result, iter, n)



class StringIteratorIO(io.TextIOBase):
    """
    File-like object fed by iterator.

    Needs to implement read() and readline().
    """
    def __init__(self, iterator):
        self._iterator = iterator
        self.tail = ''

    def readable(self):
        return True

    def seekable(self):
        return False

    def writable(self):
        return False
                
    def read(self, size=-1):
        """
        Read and return at most size characters from the stream as a single str.
        If size is negative or None, reads until EOF.
        """
        if size > -1:
            if len(self.tail) >= size:
                result = self.tail[:size]
                self.tail = self.tail[size:]
                return result
            else:
                result, self.tail = sub_read(self.tail, self._iterator, size)
                return result
        elif size < 0 or size is None:
            result = self.tail
            while not (EOF := False):
                try:
                    next_ = next(self._iterator)
                except StopIteration:
                    self.tail = ''
                    return result
                else:
                    result += next_
            self.tail = ''
            return result
        else:
            raise ValueError("size must be an integer or None")
                
    def readline(self, size=-1):
        """
        Read until newline or EOF and return a single str. If the stream
        is already at EOF, an empty string is returned.

        If size is specified, at most size characters will be read.
        """
        raise NotImplementedError
