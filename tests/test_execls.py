#! python
# -*- coding: utf-8 -*-
__author__ = "caiwanpeng"

"""测试execl工具"""

import unittest
from caibox.execl_utils import ExportTest

class TestExecl(unittest.TestCase):

    def test_create_execl(self):
        name = "测试文件"
        data = [["标题"], ["name", "age", "hight"], ["才玩彭", 18, 90], ["fdf", 60, 130]]
        manager = ExportTest(name=name, data=data)
        print(manager.get_excel_url())


if __name__ == "__main__":
    unittest.main(verbosity=1)
