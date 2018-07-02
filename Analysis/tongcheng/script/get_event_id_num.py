def get_event_id_num(data, source):
    eventID = Set()
    for i in data.index:
        if data.ix[i]["query_source"] in source:
            eventID = eventID.union(Set([data.ix[i]["query_event_id"]]))
        if data.ix[i]["doc_source"] in source:
            eventID = eventID.union(Set([data.ix[i]["doc_event_id"]]))
    return len(eventID)