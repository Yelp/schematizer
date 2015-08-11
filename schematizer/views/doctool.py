# -*- coding: utf-8 -*-
import yelp_main_internalapi.stub as yelp_main_internalapi
from pyramid.view import view_config
from yelp_lib.decorators import memoized


@memoized
def get_yelp_main_internalapi_stub():
    return yelp_main_internalapi.YelpMainInternalAPIStub(source="schematizer")


def get_admin_user_id_from_request(request):
    """Get admin_user_id from request headers. Admin user id is set by stargate
    as 'X-User-Id' in the header. See http://y/stargate_runbook for more info.
    """
    admin_user_id = request.headers.get('X-User-Id')
    if admin_user_id:
        return int(admin_user_id)

    return None


def get_admin_user_info(admin_user_id):
    """ Return the AdminUser dictionary for a given admin_user_id.

    :param int admin_user_id: The admin user id to look up.
    :return: An AdminUser dictionary model, for more information see
        yelp-main/yelp/component/internalapi_spec/swagger_1.2/admin.json
        Returns an empty dictionary if the provided admin_user_id is falsey
    """
    if not admin_user_id:
        return {}
    return get_yelp_main_internalapi_stub().get_admin_user_info(
        admin_user_id=admin_user_id
    )()


@view_config(
    route_name='doctool.index',
    request_method='GET',
    renderer='schematizer:templates/index.mako'
)
def doctool_index(request):
    """ This is invoked to serve the index of the documentation tool and extract
    authentication information from stargate.
    """
    admin_user_info = get_admin_user_info(
        admin_user_id=get_admin_user_id_from_request(request)
    )
    return {'user_email': admin_user_info.get('email')}
