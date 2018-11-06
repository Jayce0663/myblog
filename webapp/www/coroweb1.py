import functools, asyncio, inspect, logging
import os

from urllib import parse

from aiohttp import web

'''
使用inspect模块，检查视图函数的参数
'''

def get(path):
    '''
    Define decorator @get（'/path')
    :param path:
    :return:
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper.__method__ = 'GET'
        wrapper.__route__ = path
        return wrapper
    return decorator

def post(path):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper.__method__='POST'
        wrapper.__route__ = path
        return wrapper
    return decorator

# 获取无默认值的命名关键词参数
def get_required_kw_args(fn):
    args = []
    '''
     def foo(a, b = 10, *c, d,**kw): pass 
    sig = inspect.signature(foo) ==> <Signature (a, b=10, *c, d, **kw)> 
    sig.parameters ==>  mappingproxy(OrderedDict([('a', <Parameter "a">), ...])) 
    sig.parameters.items() ==> odict_items([('a', <Parameter "a">), ...)]) 
    sig.parameters.values() ==>  odict_values([<Parameter "a">, ...]) 
    sig.parameters.keys() ==>  odict_keys(['a', 'b', 'c', 'd', 'kw']) 
    '''
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        # 如果视图函数存在命名关键字参数，且默认值为空，获取她的key
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default == inspect.Parameter.empty:
            args.append(name)
    return tuple(args)

# 获取命名关键词参数
def get_named_kw_args(fn):
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            args.append(name)
    return tuple(args)

# 判断是否有命名关键词参数
def has_named_kw_args(fn):
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            return True

def has_var_kw_arg(fn): # 判断是否有关键词参数
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True

# 判断是否含有request的参数，且位置在最后
def has_request_args(fn):
    sig = inspect.signature(fn)
    params = inspect.signature(fn).parameters
    found = False
    for name, param in params.items():
        if name == 'request':
            found = True
            continue
        if found and(
            param.kind != inspect.Parameter.VAR_KEYWORD and
            param.kind != inspect.Parameter.KEYWORD_ONLY and
            param.kind != inspect.Parameter.VAR_KEYWORD
        ):
            raise ValueError('request parameter must be the last named parameter in function:%s%s'%(fn.__name__, str(sig)))
    return found

class RequestHandler(object):
    '''
    从视图函数中分析其需要接受的参数，从web.Request中获取必要的参数
    调用视图函数，然后把结果转化为web.Response对象，符合aiohttp框架要求
    '''
    def __init__(self, app, fn):
        print('request_handler')
        self._app = app
        self._func = fn
        self._required_kw_args = get_required_kw_args(fn)
        print('++++++++++++++++')
        print(self._required_kw_args)
        self._name_kw_args = get_named_kw_args(fn)
        print(self._name_kw_args)
        self._has_request_arg = has_request_args(fn)
        print(self._has_request_arg)
        self._has_named_kw_arg = has_named_kw_args(fn)
        print(self._has_named_kw_arg)
        self._has_var_kw_arg = has_var_kw_arg(fn)
        print(self._has_var_kw_arg)
        print('+++++++++++++++++')



    async def __call__(self, request):
        kw = None
        if self._has_named_kw_arg or self._has_var_kw_arg:
            if request.method == 'POST':
                if request.content_type == None:
                    return web.HTTPBadGateway(text='Missing Content_Type.')
                ct = request.content_type.lower()
                if ct.startwith('application/json'):
                    params = await request.json()
                    if not isinstance(params, dict):
                        return web.HTTPBadRequest(text='JSON body must be object')
                    kw = params
                # form表单请求的编码形式
                elif ct.startwith('application/x-www-form-urlencoded') or ct.startwith('multipart/form-data'):
                    params = await request.post()
                    kw = dict(**params)
                else:
                    return web.HTTPBadRequest(text='Unsupported Content-Type:%s' % request.content_type)
            if request.method == 'GET':
                print('entry get')
                qs = request.query_string
                if qs:
                    kw = dict()
                    for k, v in parse.parse_qs(qs, True).items():
                        kw[k] = v[0]
            if kw is None:
                '''
                request.match_info返回dict对象，可变路由中的可变字段{variable}为参数名，传入request请求的path
                '''
                kw = dict(**request.match_info)
                print('in111111')
            else:
                if self._has_named_kw_arg and (not self._has_var_kw_arg):
                    copy = dict()
                    for name in self._name_kw_args:
                        if name in kw:
                            copy[name] = kw[name]
                    kw = copy

                for k, v in request.match_info.items():
                    if k in kw:
                        logging.warn('Dupilicate arg name in name arg and kw args:%s' %k)
                    kw[k] = v
            if self._has_request_arg:
                kw['request'] = request

            if self._required_kw_args:
                for name in self._required_kw_args:
                    if not name in kw:
                        return web.HTTPBadRequest('Missing argument:%s' % name)

            logging.info('call with args:%s'% str(kw))

            r = await self._func(**kw)
            return r


def add_route(app, fn):
    method = getattr(fn, '__method__',None)
    path = getattr(fn, '__route__', None)
    if path is None or method is None:
        raise ValueError('@get or @post not defined in %s.' % str(fn))
    if not asyncio.iscoroutinefunction(fn) and not inspect.isgenerator(fn):
        fn = asyncio.coroutine(fn)
    logging.info('add route %s %s => %s(%s)'%(method, path, fn.__name__, ','.join(inspect.signature(fn).parameters.keys())))
    app.router.add_route(method, path, RequestHandler(app, fn))



def add_routes(app, module_name):
    n = module_name.rfind('.')
    if n == (-1):
        mod = __import__(module_name, globals(), locals())
    else:
        name = module_name[(n+1):]
        mod = getattr(__import__(module_name[:n], globals(), locals(), [name], 0), name)
    for attr in dir(mod):  # dir()迭代除mod模块中所有的类，实例，及函数对象（str）
        if attr.startswith('_'):
            continue
        fn = getattr(mod, attr)
        # 确保是函数
        if callable(fn):
            # 确保视图函数存在method和path
            method = getattr(fn, '__method__', None)
            path = getattr(fn, '__route__', None)
            if method and path:
                add_route(app, fn)
                print(fn.__name__)

def add_static(app):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'static')
    app.router.add_static('/static/', path)
    logging.info('add static %s => %s'%('/static/', path))
#
# def init_jinja2(app, **kw):
#     logging.info('init jinjia2...')
#     options = dict(
#         autoescape = kw.get('autoescape', True),
#         block_start_string = kw.get('block_start_string', '{%'),
#         block_end_string = kw.get('block_end_string', '%}'),
#         varibale_start_string = kw.get('variable_start_string','{{'),
#         variable_end_string = kw.get('variable_end_string', '}}'),
#         auto_reload = kw.get('auto_reload', True)
#     )
#     path = kw.get('path', None)
#     if not path:
#         path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'templates')
