#! python
# -*- coding: utf-8 -*-
__author__ = "caiwanpeng"

"""py3使用xlsxwriter生成execl的框架"""

import sys
import os, datetime
import types
import logging

import xlsxwriter

PY = sys.version_info[0]
if int(PY) == 2:
    basestring = basestring
    unicode = unicode
    from StringIO import StringIO
else:
    basestring = str
    unicode = str
    from io import StringIO


class XlsxWriterToExport(object):
    export_name = None
    file_dir = None
    is_private = True

    def __init__(self, data=[], export_name=None, create=True, create_name=None, **kwargs):
        """
        @desc 使用XlsxWriter模块生成execl表格
        :param data: 需要填充到execl的数据
        :param export_name: 文件的名字
        :param create: 是将生成的文件放在磁盘还是内存，默认磁盘
        :param create_name:
        """
        self.data = data
        self.is_close = False
        self.create = create
        self.create_name = create_name
        self.kwargs = kwargs
        self.res = None

        if export_name:
            self.export_name = export_name
        else:
            self.export_name = self.get_export_name()

        if create:
            self.export_name = u'{}.xlsx'.format(self.export_name)
            # 创建本地文件句柄
            self.output = os.path.join(os.getcwd(), self.file_dir, self.export_name)

            excel_path = os.path.dirname(self.output)
            if not os.path.isdir(excel_path):
                os.makedirs(excel_path)
        else:
            self.output = StringIO()

        self.workbook = xlsxwriter.Workbook(self.output)
        self.add_formats()
        self.create_excel()

    def add_formats(self):
        """
        @desc 设置execl的格式
        :return:
        """
        self.title_format = self.workbook.add_format(
            {'align': 'center', 'valign': 'vcenter', 'font_size': 20, 'bold': True})  # 顶部大标题
        self.head_format = self.workbook.add_format(
            {'align': 'center', 'valign': 'vcenter', "bg_color": "#ebf5fa", 'border': 1})  # 表头格式
        self.left_content = self.workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})  # 文字格式
        self.center_content = self.workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})  # 文字格式居中
        self.percent_format = self.workbook.add_format(
            {'num_format': '0.00%;[Red]-0.00%;_ * -??_ ;_ @_ ', 'border': 1})  # 比例格式
        self.int_format = self.workbook.add_format(
            {'num_format': '_ * #,##0_ ;_ * -#,##0_ ;_ * -??_ ;_ @_ ', 'border': 1})  # 整型格式
        self.money_format = self.workbook.add_format(
            {'num_format': '_ * #,##0.00_ ;_ * -#,##0.00_ ;_ * -??_ ;_ @_ ', 'border': 1})  # 浮点型格式
        self.date_format = self.workbook.add_format(
            {'num_format': 'yyyy-m-d h:mm:ss', 'align': 'left', 'valign': 'vcenter', 'border': 1})  # 日期

    def add_sheets(self):
        # 将data数据填充到execl中
        ws = self.workbook.add_worksheet(self.export_name)
        for row, item in enumerate(self.data):
            if row > 1:
                ws.write_row(row, 0, item, self.workbook.add_format({"font_color": "#333333"}))
            else:
                ws.write_row(row, 0, item)
        self.set_ws_format(ws, len(self.data[1]), title_name=self.export_name)

    def get_execl_data(self):
        # 获取execl的数据
        pass

    def date_to_string(self, value):
        if not value:
            return ""
        return value.strftime("%Y-%m-%d")

    @staticmethod
    def get_object_value(obj, code, default=None):
        '''获取表格传入对象的值'''
        # 值是函数时，避免报异常
        if isinstance(code, (types.FunctionType, types.MethodType)):
            try:
                value = code(obj)
            except:
                value = default
        elif isinstance(obj, dict):
            value = obj.get(code, default)
        else:
            value = getattr(obj, code, default)
        return value

    def create_excel(self):
        u'''生成excel内容'''
        self.add_sheets()
        self.workbook.close()
        self.get_record()

    def get_record(self):
        # 获取到生成的文件网址或二进制流
        if self.create:
            file_path = os.path.join(os.getcwd(), self.file_dir, self.export_name)
            # 返回文件地址，实际使用重写
            self.res = file_path
        else:
            export_name = self.export_name
            if not export_name.endswith('.xlsx'):
                export_name = u'%s.xlsx' % export_name
            # 返回二进制流
            self.res = self.get_output()

    def get_export_name(self):
        """
        @desc 获取文件名称，可重写此接口
        :return:
        """
        if not self.export_name:
            raise ValueError("需要配置导出文件名称：export_name")
        return self.export_name

    def get_output(self):
        u'''返回生成的excel二进制内容'''
        self.output.seek(0)
        return self.output.read()

    def get_response(self, create=True):
        # 构建二进制流的http对象
        # res = HttpResponse(self.get_output(),content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        # res['Content-Disposition'] = "attachment; filename={}.xlsx".format(self.get_export_name())
        return

    def merge_cell(self, ws, options):
        """
        @desc 合并单元格
        :param ws:
        :param options:
        :return:
        """
        for i in options:
            ws.merge_range(i["cell"], i["content"], i.get("format", None))

    def excel_style(self, row, col):
        """ 用行列数量获取excel坐标. """
        LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        result = []
        while col:
            col, rem = divmod(col - 1, 26)
            result[:0] = LETTERS[rem]
        return ''.join(result)

    def get_excel_url(self):
        return self.res

    def close(self):
        if not self.is_close:
            self.workbook.close()
            if isinstance(self.output, StringIO):
                self.output.close()
            del self.data
            del self.output
            del self.workbook
            self.is_close = True

    def __del__(self):
        # 释放资源
        self.close()
        # 删除本地文件
        try:
            if self.file_dir and self.export_name:
                path = os.path.join(os.getcwd(), self.file_dir, self.export_name)
                if os.path.exists(path):
                    os.remove(path)
        except Exception as e:
            logging.error("delete excel file is fail, error msg: {}".format(str(e)[:200]))

    def add_table_head(self, worksheet, options=None, parent_head='', title=None, child_list=None):
        '''
        加入表头
        :param worksheet: 标签页
        :param options: 表格配置信息。head_row:表头占多少行(默认占2行)， first_row:开始行号(-1表示只有一行表头)，first_col:开始列号，head_format:表头格式，source:数据源，content_format:内容格式，head_datas:表头内容
        :param parent_head: 一级表头的显示文字
        :param title: 标题的批注
        :param child_list: 这一列(或者多列)的内容信息。 code:数据源里的字段名；default:默认值；head:二级表头的显示文字(可以是空字符串)；width:宽度；format:内容样式、格式；type:sum 表示累计，index 表示序号；title:批注。
        '''
        options = options or {}
        head_row = options.get('head_row', 2)
        first_row = options.get('first_row', 0)
        first_col = options.get('first_col', 0)
        head_format = options.get('head_format', None)
        content_format = options.get('content_format', None)
        head_datas = options.get('head_datas', [])

        child_list = [child_list] if isinstance(child_list, dict) else child_list
        # 表头只占一行的
        if first_row == -1:
            pass
        # 表头只占一行的
        elif head_row == 1:
            first_row -= 1
        # 合并一级表头
        else:
            if len(child_list) == 1:
                if child_list[0].get('head'):
                    # worksheet.write(first_row, first_col, parent_head, head_format) # 无合并
                    head_datas.append(
                        {'type': 'write', "cell": (first_row, first_col), "content": parent_head, "format": head_format,
                         'comment': title})
                else:
                    # worksheet.merge_range(first_row, first_col, first_row+1, first_col, parent_head, head_format)
                    head_datas.append({'type': 'merge', 'cell': (first_row, first_col, first_row + 1, first_col),
                                       "content": parent_head, "format": head_format, 'comment': title})
            else:
                # worksheet.merge_range(first_row, first_col, first_row, first_col + len(child_list) - 1, parent_head, head_format)
                head_datas.append(
                    {'type': 'merge', 'cell': (first_row, first_col, first_row, first_col + len(child_list) - 1),
                     "content": parent_head, "format": head_format, 'comment': title})
        for child in child_list:
            # 表格内容
            options.setdefault('codes', []).append(child.get('code'))
            options.setdefault('defaults', []).append(child.get('default', ''))
            width = child.get('width', None)
            if width is None:
                width = len(unicode(child.get('head', parent_head))) * 2
                width = 12 if width < 12 else width
            column = {'width': width, "format": child.get('format', content_format)}
            options.setdefault('columns', []).append(column)
            options.setdefault('type', []).append(child.get('type'))
            # 设置列宽
            worksheet.set_column(first_col, first_col, width=width)
            # 二级表头
            # worksheet.write(first_row + 1, first_col, child.get('head', ''), head_format)
            head_datas.append(
                {'type': 'write', "cell": (first_row + 1, first_col), "content": child.get('head', parent_head),
                 "format": head_format, 'comment': child.get('title', title)})
            first_col += 1

        options['first_col'] = first_col
        options['head_datas'] = head_datas

    def add_table_data(self, worksheet, options, add_sum_line=True):
        '''
        加入表格信息
        :param worksheet: 标签页
        :param options: 表格配置信息。first_row:开始行号；first_col:开始列号；head_format:表头格式；source:数据源；codes:数据源里的字段名；defaults:默认值列表；columns:内容样式、格式；type:sum 表示累计，index 表示序号。
        :param add_sum_line: 是否需要加上统计行
        '''
        data = []
        head_row = options.get('head_row', 2)
        first_row = options.get('first_row', 0)
        columns = options.get('columns', [])
        codes = options.get('codes', [])
        defaults = options.get('defaults', [])
        col_type = options.get('type', [])
        source = options.get('source', self.data)
        head_datas = options.get('head_datas', [])
        # 各行数据
        col_index = 0
        for obj in source:
            tem = []
            for index in range(len(codes)):
                code = codes[index]
                default = defaults[index]
                this_type = col_type[index]
                # 序号
                if this_type == 'index':
                    col_index += 1
                    tem.append(col_index)
                else:
                    value = self.get_object_value(obj, code, default)
                    tem.append(value)
            data.append(tem)
        # 统计行
        if add_sum_line:
            count_data = []
            for index in range(len(codes)):
                code = codes[index]
                this_type = col_type[index]
                if index == 0:
                    count_data.append(u'合计：')
                elif this_type == 'sum':
                    amount = sum([float(self.get_object_value(obj, code, 0)) for obj in source])
                    count_data.append(amount)
                else:
                    count_data.append(u'')
            data.append(count_data)

        row_index = first_row + 1 if head_row == 1 else first_row + 2
        for row_data in data:
            for col_index in range(len(codes)):
                format = columns[col_index].get('format')
                value = row_data[col_index]
                worksheet.write(row_index, col_index, value, format)
            row_index += 1

        # 写表头
        for head_option in head_datas:
            c_type = head_option.get('type')
            cell = head_option.get('cell')
            content = head_option.get('content')
            format = head_option.get('format')
            title = head_option.get('comment')
            if isinstance(cell, basestring):
                param = (cell, content, format)
            elif isinstance(cell, (list, tuple)):
                param = list(cell) + [content, format]
            else:
                raise RuntimeError(u'传递错误参数！')
            if 'write' == c_type:
                worksheet.write(*param)
            elif 'merge' == c_type:
                worksheet.merge_range(*param)
            if title:
                worksheet.write_comment(cell[0], cell[1], title)

    def set_ws_format(self, ws, col_count,
                      title_name=None, is_title=True, title_format=None,
                      field_format=None, extral_format=None):
        """
        @desc 设置格式
        :param ws:
        :param col_count: 列数
        :param title_name: 文件标题
        :param is_title: 是否有标题
        :param title_format: 标题格式
        :param field_format: 索引列的格式
        :param extral_format: 额外的格式
        :return:
        """
        col = self.excel_style(None, col_count)

        # 如果存在标题
        if is_title:
            title_hight = 36
            base_title_format = {'align': 'center', 'valign': 'vcenter', 'font_size': 15, "font_color": '#428bca'}
            if title_format and isinstance(title_format, dict):
                title_hight = title_format.pop("hight", None) or title_hight
                base_title_format.update(title_format)
            ws.set_row(0, title_hight)  # 设置第0行的行高
            # 和合并标题单元格
            title_col = col if col_count <= 13 else "M"
            merge_obj = [
                {"cell": "A1:{}1".format(title_col), "content": title_name or "样本标题",
                 "format": self.workbook.add_format(base_title_format)}
            ]
            self.merge_cell(ws, merge_obj)
            # 字段的行号
            field_row = 1
        else:
            field_row = 0

        # 设置字段列的格式
        base_format = {'bold': True, "font_size": 12, "bg_color": "#2cb044", 'align': 'center'}
        if field_format and isinstance(field_format, dict):
            base_format.update(field_format)
        # 设置格式
        ws.set_row(field_row, None, self.workbook.add_format(base_format))
        base_extral = {"field_width": 15, "freeze_row": 2-1, "freeze_col": 2-1 }
        if extral_format and isinstance(extral_format, dict):
            base_extral.update(extral_format)
        ws.set_column("B:{}".format(col), base_extral.get("field_width"))  # 设置有数据的区域的列宽
        ws.autofilter("A{}:{}{}".format(field_row + 1, col, field_row + 1))  # 设置自动筛选区域,也就是在execl上可以点击自动筛选数据的列
        ws.freeze_panes(base_extral.get("freeze_row"), base_extral.get("freeze_col"))  # 设置冻结区域


class ExportTest(XlsxWriterToExport):
    file_dir = "basic_dir\\"

    def __init__(self, name, data=None):
        """
        @desc 测试类
        :param name:
        :param data:
        """
        self.filename = name

        if data:
            self.data = data
        else:
            self.data = self.get_execl_data()

        super(ExportTest, self).__init__(data=self.data, export_name=self.filename)

    def get_execl_data(self):
        return []

    def __del__(self):
        self.close()
