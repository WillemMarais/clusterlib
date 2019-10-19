import os


def get_dirP_of__file__(__fileP_str__: str) -> str:
    return os.path.dirname(os.path.realpath(__fileP_str__))


def flatten_list_dict(itr_obj):
    """Flatten nested dictionaries and list."""

    rtn_obj_lst = []
    if isinstance(itr_obj, dict) is True:
        rtn_obj_lst += flatten_list_dict(list(itr_obj.values()))

    elif isinstance(itr_obj, list) is True:
        for _iter_obj in itr_obj:
            rtn_obj_lst += flatten_list_dict(_iter_obj)

    else:
        rtn_obj_lst = [itr_obj]

    return rtn_obj_lst
