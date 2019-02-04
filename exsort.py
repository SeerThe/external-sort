#! /usr/bin/python3
import os
import random
import string

from collections import deque


# function for test file generation
def generate_big_file(num_lines=1000000000, line_length=10, name='data.txt'):
    with open(name, 'w') as out_file:
        for _ in range(num_lines):
            random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(line_length))
            # uncomment this line for simplified debug (strings of digits only)
            # random_string = ''.join(random.choice(string.digits) for _ in range(line_length))
            out_file.write(random_string + '\n')


class Sorter:
    """
    Sort lines in file using external memory
    """
    def __init__(self, file_name: str = 'data.txt'):
        self.file_name = file_name
        self.chunks = deque()

    def _delete_chunk(self, chunk_name: str):
       os.remove(chunk_name)

    def __del__(self):
        while self.chunks:
            chunk_name = self.chunks.popleft()
            self._delete_chunk(chunk_name)

    def split(self, chunk_size: int):
        """
        Split initial file in manageable parts for sorting in RAM 
        """
        current_chunk_number = 0
        with open(self.file_name, 'r') as in_file:
            while True:
                # get and sort new chunk
                current_chunk = sorted(in_file.readlines(chunk_size))
                if not current_chunk:
                    # EOF
                    break

                # write new ordered chunk to file
                new_chunk_file_name = 'chunk_{}.data'.format(current_chunk_number)
                with open(new_chunk_file_name, 'w') as chunk_file:
                    chunk_file.writelines(current_chunk)
                # register chunk for future use and cleanup
                self.chunks.append(new_chunk_file_name)
                current_chunk_number += 1

    def merge_chunks(self, new_id: int):
        """
        Merge sorted chunks in external memory.
        This method will take (and remove from deque) two first chunks
        and append merge results to the end of chunks deque
        """
        # get chunks
        left_chunk = self.chunks.popleft()
        right_chunk = self.chunks.popleft()

        out_file_name = 'chunk_{}.data'.format(new_id)
        with open(left_chunk, 'r') as left_file, open(right_chunk, 'r') as right_file, open(out_file_name, 'w') as out_file:
            # get initial lines
            left_line = left_file.readline()
            right_line = right_file.readline()

            # loop until one file ends
            while left_line and right_line:
                if left_line < right_line:
                    out_file.write(left_line)
                    left_line = left_file.readline()
                else:
                    out_file.write(right_line)
                    right_line = right_file.readline()

            # write hanging line from last comparison
            out_file.write(left_line or right_line)

            # get file with trailing lines and append to output
            tail = left_file if left_line else right_file
            for line in tail:
                out_file.write(line)
        # our output becomes new chunk to merge
        self.chunks.append(out_file_name)
        # won't need merged chunks, delete
        self._delete_chunk(left_chunk)
        self._delete_chunk(right_chunk)

    def merge(self, result_file: str):
        """
        Wrapper function to iteratively apply merge_chunks until only one chunk remains.
        That will be our result file.
        """
        current_chunk_number = len(self.chunks)
        while len(self.chunks) > 1:
            self.merge_chunks(current_chunk_number)
            current_chunk_number += 1

        os.rename(self.chunks.pop(), result_file)

    @classmethod
    def sort(cls, file_to_sort: str, result_file: str = 'result.txt', chunk_size: int = 100000000):
        """
        Fancy wrapper for one-line sorting.
        """
        sorter = Sorter(file_to_sort)
        sorter.split(chunk_size)
        sorter.merge(result_file)

if __name__=="__main__":
    # generate_big_file()
    Sorter.sort('data.txt', 'result.txt')

