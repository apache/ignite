def obj_to_dict(obj):
    result = {'type_name': obj.type_name}
    for data in obj.schema:
        result.update({data: getattr(obj, data)})
    return result
