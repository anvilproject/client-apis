
def chunker(seq, size):
    """Works with any sequence, returns iterator of seq in size chunks"""
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))