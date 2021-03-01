import os.path
import re
import string
import urllib.parse

from astroid.node_classes import *
from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker


def is_number(text):
    """Returns True if this text is a representation of a number"""
    try:
        float(text)
        return True
    except ValueError:
        return False


def is_child_node(child, parent):
    """Returns True if child is an eventual child node of parent"""
    node = child
    while node is not None:
        if node == parent:
            return True
        node = node.parent
    return False


def has_cyrillic(text):
    return bool(re.search('[а-яА-Я]', text))


def _is_str(obj):
    """Is this a string or a unicode string?"""
    return isinstance(obj, str) and has_cyrillic(obj)


def _is_url(text):
    """
    Test if ``text`` seems to be an URL, using ``urllib.parse`` if available,
    a fall‑back otherwise. The fall back test common protocol prefixes
    and filname extensions.
    :param str text:
    :rtype: bool
    Note: this just test URL, not general URI.
    Examples which will return ``True``:
     * ``"file://localhost"``
     * ``"file://localhost/document.txt"``
     * ``"file:///document.txt"``
     * ``"file:///"``
    Samples which will return ``False``:
     * ``"document.txt"``
     * ``"/document.txt"``
     * ``"file://"``
    """

    url = urllib.parse.urlparse(text)
    has_scheme = url.scheme != ''
    has_netloc = url.netloc != ''
    has_path = url.path != ''
    result = has_scheme and (has_netloc or has_path)

    return result


def _is_path(text):
    """
    Test if ``text`` seems to be an URL, using ``urllib.parse`` if available,
    a fall‑back otherwise. The fall back test common protocol prefixes
    and filname extensions.
    :param str text:
    :rtype: bool
    This is not intended to be reliable for other mean than telling if
    whether or not, the string is translatable. Don't use this function
    for other purpose, as it may return a lot of false negative.
    Examples which will return ``True``:
     * ``"~/document.txt"``
     * ``"../document.txt"``
     * ``"something/.."``
     * ``"parent/../child"``
    Samples which will return ``False``:
     * ``"parent/child"``
     * ``"document.txt"``
    """

    result = False

    if os.path.expanduser(text) != text:
        # Expands ``$HOME`` on Windows, but not on UNIces. Still don't
        # use ``os.path.expandvars``, as this expands everything,
        # including what's not really related t paths.
        result = True
    elif text.find('./') != -1:
        # Testing ``./`` includes testing ``../``.
        result = True
    elif text.find('/.') != -1:
        # Same comment as above.
        result = True

    return result


# class MissingGettextChecker(BaseChecker):
#     """Checks for strings that aren't wrapped in a _ call somewhere."""
#
#     __implements__ = IAstroidChecker
#
#     name = 'missing_gettext'
#     msgs = {
#         'W9903': (
#             'non-gettext-ed string %r',
#             'non-gettext-string',
#             "There is a raw string that's not passed through gettext",
#         ),
#     }
#
#     # this is important so that your checker is executed before others
#     priority = -1
#
#     def visit_const(self, node):
#         if not _is_str(node.value):
#             return
#
#         # Ignore some strings based on the contents.
#         # Each element of this list is a one argument function. if any of them
#         # return true for this string, then this string is ignored
#         whitelisted_strings = [
#             # ignore empty strings
#             lambda x: x == '',
#             # some strings we use
#             lambda x: x in ['POST', 'agency', 'promoter', 'venue', 'utf-8'],
#             # This string is probably used as a key or something, and should
#             # be ignored
#             lambda x: len(x) > 3 and x.upper() == x,
#             # pure number
#             is_number,
#             # URL, can't be translated
#             _is_url,
#             # Paths, usually can't be translated
#             _is_path,
#             # probably a regular expression
#             lambda x: x.startswith("^") and x.endswith("$"),
#             # probably a URL fragment
#             lambda x: x.startswith("/") and x.endswith("/"),
#             # Only has format specifiers and non-letters, so ignore it
#             lambda x: (
#                 not any(
#                     [
#                         z in x.replace("%s", "").replace("%d", "")
#                         for z in string.ascii_letters
#                     ]
#                 )
#             ),
#             # sending http attachment header
#             lambda x: x.startswith("attachment; filename="),
#             # sending http header
#             lambda x: x.startswith("text/html; charset="),
#         ]
#
#         for func in whitelisted_strings:
#             if func(node.value):
#                 return
#
#         # Whitelist some strings based on the structure.
#         # Each element of this list is a 2-tuple, class and then a 2 arg
#         # function. Starting with the current string, and going up the parse
#         # tree to the root (i.e. the whole file), for every whitelist element,
#         # if the current node is an instance of the first element, then the
#         # 2nd element is called with that node and the original string. If
#         # that returns True, then this string is assumed to be OK.
#         # If any parent node of this string returns True for any of these
#         # functions then the string is assumed to be OK
#         whitelist = [
#             # {'shouldignore': 1}
#             (Dict, lambda curr_node, node: node in [x[0] for x in curr_node.items]),
#             # dict['shouldignore']
#             (Index, lambda curr_node, node: curr_node.value == node),
#             # list_display = [....]
#             # e.g. Django Admin class Meta:...
#             (
#                 Assign,
#                 lambda curr_node, node: (
#                     len(curr_node.targets) == 1
#                     and hasattr(curr_node.targets[0], 'name')
#                     and curr_node.targets[0].name
#                     in [
#                         'list_display',
#                         'js',
#                         'css',
#                         'fields',
#                         'exclude',
#                         'list_filter',
#                         'list_display_links',
#                         'ordering',
#                         'search_fields',
#                         'actions',
#                         'unique_together',
#                         'db_table',
#                         'custom_filters',
#                         'search_fields',
#                         'custom_date_list_filters',
#                         'export_fields',
#                         'date_hierarchy',
#                     ]
#                 ),
#             ),
#             # Just a random doc-string-esque string in the code
#             (Delete, lambda curr_node, node: curr_node.value == node),
#             # X(attrs={'class': 'somecssclass', 'maxlength': '20'})
#             (
#                 Keyword,
#                 lambda curr_node, node: (
#                     curr_node.arg == 'attrs'
#                     and hasattr(curr_node.value, 'items')
#                     and node
#                     in [
#                         x[1]
#                         for x in curr_node.value.items
#                         if x[0].value
#                         in [
#                             'class',
#                             'maxlength',
#                             'cols',
#                             'rows',
#                             'checked',
#                             'disabled',
#                             'readonly',
#                         ]
#                     ]
#                 ),
#             ),
#             # X(attrs=dict(....))
#             (
#                 Keyword,
#                 lambda curr_node, node: (
#                     curr_node.arg == 'attrs'
#                     and isinstance(curr_node.value, Call)
#                     and hasattr(curr_node.value.func, 'name')
#                     and curr_node.value.func.name == 'dict'
#                 ),
#             ),
#             # x = CharField(default='xxx', related_name='tickets') etc.
#             (
#                 Keyword,
#                 lambda curr_node, node: (
#                     curr_node.arg
#                     in [
#                         'regex',
#                         'prefix',
#                         'css_class',
#                         'mimetype',
#                         'related_name',
#                         'default',
#                         'initial',
#                         'upload_to',
#                     ]
#                     and curr_node.value == node
#                 ),
#             ),
#             (
#                 Keyword,
#                 lambda curr_node, node: (
#                     curr_node.arg in ['input_formats']
#                     and len(curr_node.value.elts) == 1
#                     and curr_node.value.elts[0] == node
#                 ),
#             ),
#             (
#                 Keyword,
#                 lambda curr_node, node: (
#                     curr_node.arg in ['fields'] and node in curr_node.value.elts
#                 ),
#             ),
#             # something() == 'string'
#             (Compare, lambda curr_node, node: node == curr_node.ops[0][1]),
#             # 'something' == blah()
#             (Compare, lambda curr_node, node: node == curr_node.left),
#             # Try to exclude queryset.extra(something=[..., 'some sql',...]
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     curr_node.func.attrname in ['extra']
#                     and any(is_child_node(node, x) for x in curr_node.args)
#                 ),
#             ),
#             # Queryset functions, queryset.order_by('shouldignore')
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     isinstance(curr_node.func, Attribute)
#                     and curr_node.func.attrname
#                     in [
#                         'has_key',
#                         'pop',
#                         'order_by',
#                         'strftime',
#                         'strptime',
#                         'get',
#                         'select_related',
#                         'values',
#                         'filter',
#                         'values_list',
#                     ]
#                 ),
#             ),
#             # logging.info('shouldignore')
#             (Call, lambda curr_node, node: curr_node.func.expr.name in ['logging']),
#             # hasattr(..., 'should ignore')
#             # HttpResponseRedirect('/some/url/shouldnt/care')
#             # first is function name, 2nd is the position the string must be
#             # in (none to mean don't care)
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     curr_node.func.name in ['hasattr', 'getattr']
#                     and curr_node.args[1] == node
#                 ),
#             ),
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     curr_node.func.name in ['HttpResponseRedirect', 'HttpResponse']
#                 ),
#             ),
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     curr_node.func.name == 'set_cookie' and curr_node.args[0] == node
#                 ),
#             ),
#             (
#                 Call,
#                 lambda curr_node, node: (
#                     curr_node.func.name in ['ForeignKey', 'OneToOneField']
#                     and curr_node.args[0] == node
#                 ),
#             ),
#         ]
#
#         string_ok = False
#
#         debug = False
#         # debug = True
#         curr_node = node
#         if debug:
#             import pdb
#
#             pdb.set_trace()
#
#         # we have a string. Go upwards to see if we have a _ function call
#         try:
#             while curr_node.parent is not None:
#                 if debug:
#                     print(repr(curr_node))
#                     print(repr(curr_node.as_string()))
#                     print(curr_node.repr_tree())
#                 if isinstance(curr_node, Call):
#                     if hasattr(curr_node, 'func') and hasattr(curr_node.func, 'name'):
#                         if curr_node.func.name in ['_', 'ungettext', 'ungettext_lazy']:
#                             # we're in a _() call
#                             string_ok = True
#                             break
#
#                 # Look at our whitelist
#                 for cls, func in whitelist:
#                     if isinstance(curr_node, cls):
#                         try:
#                             # Ignore any errors from here. Otherwise we have to
#                             # pepper the whitelist with loads of defensive
#                             # hasattrs, which increase bloat
#                             if func(curr_node, node):
#                                 string_ok = True
#                                 break
#                         except AttributeError:
#                             pass
#
#                 curr_node = curr_node.parent
#
#         except Exception as error:  # pylint: disable=W0703
#             print(node, node.as_string())
#             print(curr_node, curr_node.as_string())
#             print(error)
#             import pdb
#
#             pdb.set_trace()
#
#         if not string_ok:
#             # we've gotten to the top of the code tree / file level and we
#             # haven't been whitelisted, so add an error here
#             self.add_message('W9903', node=node, args=node.value)


class MissingGettextChecker(BaseChecker):
    """Checks for strings that aren't wrapped in a _ call somewhere."""

    __implements__ = IAstroidChecker

    name = 'missing_gettext'
    msgs = {
        'W0001': (
            'non-gettext-ed string %r',
            'non-gettext-string',
            "There is a raw string that's not passed through gettext",
        ),
    }

    # this is important so that your checker is executed before others
    priority = -1

    def visit_const(self, node):
        if not _is_str(node.value):
            return

        # Ignore some strings based on the contents.
        # Each element of this list is a one argument function. if any of them
        # return true for this string, then this string is ignored
        whitelisted_strings = [
            # ignore empty strings
            lambda x: x == '',
            # This string is probably used as a key or something, and should
            # be ignored
            lambda x: len(x) > 3 and x.upper() == x,
            # pure number
            is_number,
            # URL, can't be translated
            _is_url,
            # Paths, usually can't be translated
            _is_path,
            # probably a regular expression
            lambda x: x.startswith("^") and x.endswith("$"),
            lambda x: '[^' in x or 'а-я' in x or 'А-Я' in x,
            # probably a URL fragment
            lambda x: x.startswith("/") or x.endswith("/"),
        ]

        for func in whitelisted_strings:
            if func(node.value):
                return

        # Whitelist some strings based on the structure.
        # Each element of this list is a 2-tuple, class and then a 2 arg
        # function. Starting with the current string, and going up the parse
        # tree to the root (i.e. the whole file), for every whitelist element,
        # if the current node is an instance of the first element, then the
        # 2nd element is called with that node and the original string. If
        # that returns True, then this string is assumed to be OK.
        # If any parent node of this string returns True for any of these
        # functions then the string is assumed to be OK
        whitelist = [
            # {'shouldignore': 1}
            (Dict, lambda curr_node, node: node in [x[0] for x in curr_node.items]),
            # dict['shouldignore']
            (Index, lambda curr_node, node: curr_node.value == node),
            # list_display = [....]
            # e.g. Django Admin class Meta:...
            (
                Assign,
                lambda curr_node, node: (
                    len(curr_node.targets) == 1
                    and hasattr(curr_node.targets[0], 'name')
                    and curr_node.targets[0].name
                    in [
                        'list_display',
                        'js',
                        'css',
                        'fields',
                        'exclude',
                        'list_filter',
                        'list_display_links',
                        'ordering',
                        'search_fields',
                        'actions',
                        'unique_together',
                        'db_table',
                        'custom_filters',
                        'search_fields',
                        'custom_date_list_filters',
                        'export_fields',
                        'date_hierarchy',
                        'verbose_name',
                        'verbose_name_plural',
                    ]
                ),
            ),
            # Just a random doc-string-esque string in the code
            (Delete, lambda curr_node, node: curr_node.value == node),
            # X(attrs={'class': 'somecssclass', 'maxlength': '20'})
            (
                Keyword,
                lambda curr_node, node: (
                    curr_node.arg == 'attrs'
                    and hasattr(curr_node.value, 'items')
                    and node
                    in [
                        x[1]
                        for x in curr_node.value.items
                        if x[0].value
                        in [
                            'class',
                            'maxlength',
                            'cols',
                            'rows',
                            'checked',
                            'disabled',
                            'readonly',
                        ]
                    ]
                ),
            ),
            # X(attrs=dict(....))
            (
                Keyword,
                lambda curr_node, node: (
                    curr_node.arg == 'attrs'
                    and isinstance(curr_node.value, Call)
                    and hasattr(curr_node.value.func, 'name')
                    and curr_node.value.func.name == 'dict'
                ),
            ),
            # x = CharField(default='xxx', related_name='tickets') etc.
            (
                Keyword,
                lambda curr_node, node: (
                    curr_node.arg
                    in [
                        'regex',
                        'prefix',
                        'css_class',
                        'mimetype',
                        'related_name',
                        'default',
                        'initial',
                        'upload_to',
                        'verbose_name',
                        'help_text',
                    ]
                    and curr_node.value == node
                ),
            ),
            (
                Keyword,
                lambda curr_node, node: (
                    curr_node.arg in ['input_formats']
                    and len(curr_node.value.elts) == 1
                    and curr_node.value.elts[0] == node
                ),
            ),
            (
                Keyword,
                lambda curr_node, node: (
                    curr_node.arg in ['fields'] and node in curr_node.value.elts
                ),
            ),
            # something() == 'string'
            (Compare, lambda curr_node, node: node == curr_node.ops[0][1]),
            # 'something' == blah()
            (Compare, lambda curr_node, node: node == curr_node.left),
            # Try to exclude queryset.extra(something=[..., 'some sql',...]
            (
                Call,
                lambda curr_node, node: (
                    curr_node.func.attrname in ['extra']
                    and any(is_child_node(node, x) for x in curr_node.args)
                ),
            ),
            # Queryset functions, queryset.order_by('shouldignore')
            (
                Call,
                lambda curr_node, node: (
                    isinstance(curr_node.func, Attribute)
                    and curr_node.func.attrname
                    in [
                        'has_key',
                        'pop',
                        'order_by',
                        'strftime',
                        'strptime',
                        'get',
                        'select_related',
                        'values',
                        'filter',
                        'values_list',
                    ]
                ),
            ),
            # logging.info('shouldignore')
            (Call, lambda curr_node, node: curr_node.func.expr.name in ['logging']),
            # hasattr(..., 'should ignore')
            # HttpResponseRedirect('/some/url/shouldnt/care')
            # first is function name, 2nd is the position the string must be
            # in (none to mean don't care)
            (
                Call,
                lambda curr_node, node: (
                    curr_node.func.name in ['hasattr', 'getattr']
                    and curr_node.args[1] == node
                ),
            ),
            (
                Call,
                lambda curr_node, node: (
                    curr_node.func.name in ['HttpResponseRedirect', 'HttpResponse']
                ),
            ),
            (
                Call,
                lambda curr_node, node: (
                    curr_node.func.name == 'set_cookie' and curr_node.args[0] == node
                ),
            ),
            (
                Call,
                lambda curr_node, node: (
                    curr_node.func.name in ['ForeignKey', 'OneToOneField']
                    and curr_node.args[0] == node
                ),
            ),
        ]

        string_ok = False

        debug = False
        # debug = True
        curr_node = node
        # if debug:
        #     import pdb
        #
        #     pdb.set_trace()

        # we have a string. Go upwards to see if we have a _ function call
        try:
            while curr_node.parent is not None:
                if debug:
                    print(repr(curr_node))
                    print(repr(curr_node.as_string()))
                    print(curr_node.repr_tree())
                if isinstance(curr_node, Call):
                    if hasattr(curr_node, 'func') and hasattr(curr_node.func, 'name'):
                        if curr_node.func.name in [
                            '_',
                            'ungettext',
                            'ungettext_lazy',
                            'gettext',
                            'gettext_lazy',
                        ]:
                            # we're in a _() call
                            string_ok = True
                            break

                # Look at our whitelist
                for cls, func in whitelist:
                    if isinstance(curr_node, cls):
                        try:
                            # Ignore any errors from here. Otherwise we have to
                            # pepper the whitelist with loads of defensive
                            # hasattrs, which increase bloat
                            if func(curr_node, node):
                                string_ok = True
                                break
                        except AttributeError:
                            pass

                curr_node = curr_node.parent

        except Exception as error:
            print(node, node.as_string())
            print(curr_node, curr_node.as_string())
            print(error)
            import pdb

            pdb.set_trace()

        if not string_ok:
            # we've gotten to the top of the code tree / file level and we
            # haven't been whitelisted, so add an error here
            self.add_message('W0001', node=node, args=node.value)


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(MissingGettextChecker(linter))
