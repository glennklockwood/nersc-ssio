#!/usr/bin/env python

import hashlib

def sha1sum( f, blocksize=2**30 ):
    """Calculate the SHA1 sum of a file-like object"""
    hasher = hashlib.new('sha1')
    buf = f.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = f.read(blocksize)
    return hasher.hexdigest()

def checksum( f ):
    """Wrapper function for SHA1 sum"""
    sha1sum( f )

def checksum_file( filename ):
    with open(filename, 'rb') as f:
        checksum( f )
