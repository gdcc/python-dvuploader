class ChunkStream:
    """
    A class used to represent a Chunk Stream.

    ...

    Attributes:
        read_limit (int) - The maximum amount of data to be read from the file
        amount_seen (int) - The amount of data already read from the file
        file_obj (BufferedReader) - The file to read from
        len (int) - the length of the data to be read, same as read_limit
    """

    def __init__(self, file_obj, read_limit, file_size):
        """
        Constructs all the necessary attributes for the ChunkStream object.

        Args:
            file_obj (BufferedReader) - The file to read from
            read_limit (int) - the maximum amount of data to be read from the file
        """
        self.read_limit = read_limit
        self.amount_seen = 0
        self.file_obj = file_obj

        # So that requests doesn't try to chunk the upload but will instead stream it:
        self.len = min(file_size, read_limit)

    def read(self, amount=-1):
        if self.amount_seen >= self.read_limit:
            return b""

        remaining_amount = self.read_limit - self.amount_seen

        if amount < 0:
            amount = remaining_amount

        data = self.file_obj.read(min(amount, remaining_amount))
        self.amount_seen += len(data)
        return data
