#! python
# -*- coding: utf-8 -*-
__author__ = "caiwanpeng"

"""
基于redis的bloomFilter封装,普通的基于内存的bloomFilter实现可以使用pybloom_live这个第三方库，
redis4.0.0版本后内部支持了BloomFilter的插件，但是对于不想使用该插件或者版本低于redis4.0.0可以使用该库实现基于reids
的BloomFilter
"""

import json
import base64
import mmh3


class ReBloomFilter(object):

    def __init__(self, cache, name, size, error_rate=0.001, is_byte=True, hash_seed=41, is_init=False, expire=None):
        """
        @desc 封装基于redis的bloom过滤器
        :param init_size: 初始的过滤器长度
        :param name:redis的缓存键
        :param error_rate: 碰撞概率
        :param cache: redis连接对象
        :param is_byte: 是否自动将长度设置成8的倍数
        :param hash_seed: hash种子
        :param is_init: 是否初始化空间
        :param expire: 失效时间s
        """
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")
        if not size > 0:
            raise ValueError("size must be > 0")

        self._client = cache
        self.__size = size
        self.error_rate = error_rate
        self.hash_count = self.__get_hash_count(error_rate)
        self.__base_key = name
        self.is_byte = is_byte
        self.hash_seed = hash_seed
        self.expire = expire
        self.__get_size()

        self._init_filter(is_init=is_init)

    def _init_filter(self, is_init):
        """
        @desc 初始化
        :return:
        """
        if is_init:
            self._client.setbit(self.__base_key, self.__size, 0)

    def __get_hash_count(self, value):
        """
        @desc 获取hash函数的数量
        :param value:
        :return:
        """
        if 0.001 <= value < 0.01:
            return 3
        elif value < 0.001:
            return 4
        else:
            return 2

    def __get_size(self):
        """
        @desc 获取长度
        :return:
        """
        if self.is_byte:
            mod = self.__size % 8
            self.__size += (8 - mod) if mod else 0
        return self.__size

    def __contains__(self, key):
        """
        @desc 重写in关键字的执行逻辑, 查询是否存在
        """
        hash_l = self.__get_hash_list(key)
        pipline = self._client.pipeline()
        for k in hash_l:
            pipline.getbit(self.__base_key, k)
        return all(pipline.execute())

    def __get_hash_list(self, key):
        """
        @desc 获取hash值
        :return:
        """
        return [mmh3.hash(key, i, False) % self.__size for i in
                range(self.hash_seed, self.hash_seed + self.hash_count)]

    def set_bloom_value(self, value):
        """
        @desc 直接设置过滤器的数据
        :param value:
        :return:
        """
        self._client.set(self.__base_key, value)
        if self.expire:
            self._client.expire(self.__base_key, self.expire)

    def __len__(self):
        """重写len,计算过滤器总计数"""
        return self._client.bitcount(self.__base_key, 0, -1)

    @property
    def size(self):
        """
        @desc 返回过滤器的总大小
        :return:
        """
        return self.__size

    @property
    def count(self):
        """
        @desc 返回当前过滤器计数
        :return:
        """
        return len(self)

    @property
    def client(self):
        return self._client

    def add(self, key):
        """
        @desc 往过滤器中添加数据
        :param value:
        :return:
        """
        hash_l = self.__get_hash_list(key)
        pipe = self._client.pipeline()
        for k in hash_l:
            pipe.setbit(self.__base_key, k, 1)
        return not all(pipe.execute())

    def madd(self, keys):
        """
        @desc 批量添加数据
        :param keys:
        :return:
        """
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            self.add(key)

    def exists(self, key):
        """
        @desc 判断是否存在一个key，当返回False时一定正确，
        但是返回True是可能不正确
        :param key:
        :return:
        """
        if key in self:
            return True
        return False

    def mexists(self, keys):
        """
        @desc 批量判断key是否存在
        :param keys: list
        :return: list
        """
        if isinstance(keys, str):
            keys = [keys]
        return [self.exists(key) for key in keys]

    def union(self, other, dest):
        """
        @desc 两个过滤器合并成新的过滤器
        :param other: ReBloomFilter object
        :param dest: 新的过滤器的key
        :return:
        """
        assert isinstance(other, ReBloomFilter)
        if self.hash_count != other.hash_count or self.hash_seed != other.hash_seed or self.size != other.size:
            raise ValueError("两个过滤器的size、error_rate、hash_seed参数必须一致！")
        self._client.bitop("or", dest, self.__base_key, other.__base_key)
        new_bloom = ReBloomFilter(self._client, dest, self.size, self.error_rate, self.is_byte, self.hash_seed,
                                  is_init=False, expire=self.expire)
        return new_bloom

    def intersection(self, other, dest):
        """
        @desc 两个过滤器的交集合并成新的过滤器
        :param other: ReBloomFilter object
        :param dest: 新的过滤器的key
        :return:
        """
        assert isinstance(other, ReBloomFilter)
        if self.hash_count != other.hash_count or self.hash_seed != other.hash_seed or self.size != other.size:
            raise ValueError("两个过滤器的size、error_rate、hash_seed参数必须一致！")
        self._client.bitop("and", dest, self.__base_key, other.__base_key)
        new_bloom = ReBloomFilter(self._client, dest, self.size, self.error_rate, self.is_byte, self.hash_seed,
                                  is_init=False, expire=self.expire)
        return new_bloom

    def copy(self, dest):
        """
        @desc 复制一个过滤器
        :param dest: 新的过滤器的key
        :return:
        """
        zero_key = "{}:{}".format(dest, 0)
        zero_bloom = ReBloomFilter(self._client, zero_key, self.size, self.error_rate, self.is_byte, self.hash_seed,
                                   is_init=False, expire=self.expire)
        copy_bloom = self.union(zero_bloom, dest=dest)
        self._client.expire(zero_key, -1)
        return copy_bloom

    def tofile(self, f):
        """
        @desc 将bloomFilter保存到文件,为了便于理解，使用json序列化
        :param f: file对象
        :return:
        """
        assert isinstance(f, file)
        bloom_data = base64.b64encode(self._client.get(self.__base_key))
        data = json.dumps({
            "hash": "mmh3",
            "size": self.size,
            "error_rate": self.error_rate,
            "is_byte": self.is_byte,
            "hash_seed": self.hash_seed,
            "is_init": False,
            "name": self.__base_key,
            "expire": self.expire,
            "bloom_data": bloom_data
        })
        f.write(data)

    @classmethod
    def fromfile(self, f, cache):
        """
        @desc 从文件中读取创建一个bloomFilter
        :param f: file对象
        :param cache: redis连接对象
        :return:
        """
        assert isinstance(f, file)
        data = json.loads(f.read())
        if data["hash"] != "mmh3":
            raise ValueError("the data'hash is must be mmh3")
        data.pop("hash", None)
        data["cache"] = cache
        data_bloom = base64.b64decode(data.pop("bloom_data", ""))
        bloom = ReBloomFilter(**data)
        bloom.set_bloom_value(data_bloom)
        return bloom


class ScalableReBloomFilter(object):

    def __init__(self, cache, name, init_size, error_rate=0.001, is_byte=True):
        """
        @desc 封装基于redis的自动扩容的bloom过滤器
        :param init_size: 初始的过滤器长度
        :param name:redis的缓存键
        :param error_rate: 碰撞概率
        :param cache: redis连接对象
        :param is_byte: 是否自动将长度设置成8的倍数
        """
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")
        if not init_size > 0:
            raise ValueError("init_size must be > 0")

        self._client = cache
        self.__size = init_size
        self.__hash_count = self.__get_hash_count(error_rate)
        self.__base_key = name
        self.__is_byte = is_byte

        self.filters = []
        self._init_filter()
        self.__get_size()

    def _init_filter(self):
        """
        @desc 初始化空间
        :return:
        """
        self.__key1 = "{}:{}".format(self.__base_key, 0)
        self.__base_filter = self._client.setbit(self.__key1, self.__size, 0)
        self.filters.append(self.__base_filter)

    def __get_hash_count(self, value):
        """
        @desc 获取hash函数的数量
        :param value:
        :return:
        """
        if 0.001 <= value < 0.01:
            return 3
        elif value < 0.001:
            return 4
        else:
            return 2

    def __get_size(self):
        """
        @desc 获取长度
        :return:
        """
        if self.__is_byte:
            self.__size += 8 - self.__size % 8
        return self.__size

    def __contains__(self, key):
        """
        @desc 重写in关键字的执行逻辑
        """
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def __len__(self):
        """重写len,计算过滤器总计数"""
        return sum(f.bitcount("{}:{}".format(self.__base_key, i), 0, -1) for i, f in enumerate(self.filters))

    @property
    def size(self):
        """
        @desc 返回过滤器的总大小
        :return:
        """
        pass

    @property
    def count(self):
        """
        @desc 返回当前过滤器计数
        :return:
        """
        return len(self)

    def add(self, value):
        """
        @desc 往过滤器中添加数据
        :param value:
        :return:
        """
        pass
