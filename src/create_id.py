import datetime
import copy


class ID:

    def __init__(self, time, id):
        self.__time = time
        self._id = id

    def get_time(self):
        return self.__time

    def set_time(self, time):
        self.__time = time

    def get_id(self):
        return self.__id

    def set_time(self, time):
        self.__time = time

    def set_id(self, id):
        self.__id = id

    def assign_id(self):
        now = datetime.datetime.now()
        new_time = now.strftime("%Y%m%d%H%M%S")

        if new_time == ID.get_time(self):
            current_id =  ID.get_id(self)
            splitted = (current_id.split('_')[2])
            order = splitted.lstrip('0')
            order_int = int(order)
            counter = order_int + 1
            es_des_id: str = 'es_' + new_time + ('_') + (str(counter).zfill(6))

        else:
            es_des_id: str = 'es_' + new_time + ('_') + (str(1).zfill(6))
        ID.set_time(self, new_time)
        ID.set_id(self, es_des_id)
        return es_des_id
