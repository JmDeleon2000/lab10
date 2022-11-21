from bitarray import bitarray
from bitarray.util import int2ba
from bitarray.util import ba2int
import numpy as np


def pack(temperatura, direccion, humedad):
    maskhum = bitarray('000111111100000000000000')
    humbuff = (int2ba(humedad, 24)<<14)&maskhum
    temp = bitarray(buffer=np.array([temperatura], dtype=np.float16))

    x = (temp[1::]+bitarray('000000000')>>10)&bitarray('000000000011111110000000')
    y = temp[9::]+bitarray('00000000000000000')>>17

    tempbuff = x|y
    dirbuff = int2ba(direccion, 24)<<21
    return humbuff|dirbuff|tempbuff



def unpack(packed):
    dir = ba2int(packed[:3:])

    hum = ba2int(packed[3::]>>14)


    buff = bitarray(16)
    buff.setall(0)
    x=((packed[10::]+bitarray('00'))>>1)&bitarray('0111111100000000')

    y=(packed[17::]+bitarray('000000000'))>>9

    buff = buff|x|y
    temp = np.frombuffer(buff, dtype=np.half)
    return temp[0], dir, hum


