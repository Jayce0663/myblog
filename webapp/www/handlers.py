from www.coroweb1 import get

from www.models import User


@get('/')
async def index(request):
    users = await User.findAll()
    return {
        '__template__': 'test.html',
        'users':users
    }