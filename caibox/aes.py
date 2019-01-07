# -*- coding: utf-8 -*-
# !/usr/bin/python
"""
-------------------------------------------------
   File Name：     secrets
   Description :  AES对称加密方法python实现
   Author :       caiwanpeng
   date：          2019/1/2
-------------------------------------------------
   Change Activity:
                   2019/1/2:
-------------------------------------------------
"""
__author__ = 'caiwanpeng'

__version__ = '1.1'

import base64
import platform
import os

if platform.system() == "Windows":
    from Cryptodome.Cipher import AES
    from Cryptodome import Random
    from Cryptodome.Util import Counter
if platform.system() == "Linux":
    from Crypto.Cipher import AES
    from Crypto import Random
    from Crypto.Util import Counter


class BasePrpcrypt(object):
    """
    AES加密基础类
    """

    def encrypt(self, text: str):
        """
        加密函数
        :param text: 明文
        :return: 密文
        """
        pass

    def decrypt(self, text: bytes):
        """
        解密函数
        :param text: 密文
        :return: 明文
        """
        pass


class ECBPrpcrypt(BasePrpcrypt):
    """
    AES加密的ECB加密模式
    "\0"补位，base64编码
    """

    def __init__(self, key):
        if not isinstance(key, str) or len(key) % 16 != 0:
            ValueError("key参数必须是字符串和16的倍数！")
        self.key = key.encode('utf-8')  # 先用utf-8编码转成二进制数据
        self.mode = AES.MODE_ECB
        self.length = 16

    def encrypt(self, text: str):
        """
        加密函数
        :param text: 需要加密的数据，要求必须是字符串，即是序列化后的数据
        :return: 密文
        """
        cryptor = AES.new(self.key, self.mode)
        # 计算需要补位的字节数
        add = self.length - (len(text) % self.length)
        text = text + ('\0' * add)  # 补位
        print(text.encode('utf-8'))
        # 加密后得到二进制的密文，为了网络传输的需要，使用base64进行编码防止出现乱码
        return base64.b64encode(cryptor.encrypt(text.encode('utf-8')))

    def decrypt(self, text: bytes):
        """
        解密函数
        :param text: 加密后的二进制密文
        :return:明文str
        """
        cryptor = AES.new(self.key, self.mode)
        plain_text = cryptor.decrypt(base64.b64decode(text))
        # 将二进制的明文解码去掉补位的数据
        return plain_text.decode("utf-8").rstrip('\0')


class CTRPrpcrypt(BasePrpcrypt):
    """
    AES的CTR模式实现
    "\0"补位，base64编码
    """

    def __init__(self, key):
        """
        需要密匙
        :param key: 密匙
        """
        if not isinstance(key, str) or len(key) % 16 != 0:
            ValueError("key参数必须是字符串和16的倍数！")
        self.key = key.encode('utf-8')
        self.mode = AES.MODE_CTR
        self.length = 16

    def encrypt(self, text: str):
        """
        加密函数
        :param text:
        :return:
        """
        # 补码
        add = self.length - (len(text) % self.length)
        text = text + ('\0' * add)

        # 算子表
        nonce = os.urandom(8)
        countf = Counter.new(64, nonce)

        encrypto = AES.new(self.key, AES.MODE_CTR, counter=countf)
        return base64.b64encode(nonce + encrypto.encrypt(text.encode()))

    def decrypt(self, text: bytes):
        """
        解密函数
        :param text:
        :return:
        """
        text = base64.b64decode(text)
        countf = Counter.new(64, text[:8])

        encrypto = AES.new(self.key, AES.MODE_CTR, counter=countf)
        plain_text = encrypto.decrypt(text[8:])
        return plain_text.decode().rstrip('\0')


class Prpcrypt(BasePrpcrypt):
    """
    AES的CBC\CFB\OFB模式实现
    "\0"补位，base64编码
    """

    def __init__(self, key, mode):
        """
        需要密匙和加密模式
        :param key: 密匙
        :param mode: 加密模式
        CBC模式：2，
        CFB模式：3，
        OFB模式：5，
        """
        if not isinstance(key, str) or len(key) % 16 != 0:
            ValueError("key参数必须是字符串和16的倍数！")
        self.key = key.encode('utf-8')
        self.mode = mode
        self.length = 16

    def encrypt(self, text: str):
        """
        加密函数
        :param text:
        :return:
        """
        # 获取16字节二进制密匙向量，其不能被解码，这个向量起扰乱作用
        iv = Random.new().read(AES.block_size)
        crypto = AES.new(self.key, self.mode, iv)
        add = self.length - (len(text) % self.length)
        text = text + '\0' * add
        return base64.b64encode(iv + crypto.encrypt(text.encode("utf-8")))

    def decrypt(self, text: bytes):
        """
        解密函数
        :param text:被加密的二进制密文
        :return:明文
        """
        b_text = base64.b64decode(text)
        cryptor = AES.new(self.key, self.mode, b_text[:16])
        plain_text = cryptor.decrypt(b_text[16:])
        return plain_text.decode().rstrip('\0')


if __name__ == "__main__":
    pass
