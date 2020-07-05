import datetime
import copy


class ID:

    def __init__(self, time, id, jax_id, do_id, go_id, onco_id):
        self.__time = time
        self._id = id
        self._jax_id = jax_id
        self._do_id = do_id
        self._go_id = go_id
        self._onco_id = onco_id

    def get_time(self):
        return self.__time

    def set_time(self, time):
        self.__time = time

    def get_id(self):
        return self.__id

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

    def get_jax_id(self):
        self.set_jax_id( )
        return self._jax_id

    def set_jax_id( self):
        self._jax_id = self._jax_id + 1

    def get_do_id(self):
        self.set_do_id()
        return self._do_id

    def set_do_id(self):
        self._do_id = self._do_id + 1

    def get_go_id(self):
        self.set_go_id()
        return self._go_id

    def set_go_id(self):
        self._go_id = self._go_id + 1

    def get_onco_id(self):
        self.set_onco_id()
        return self._onco_id

    def set_onco_id(self):
        self._onco_id = self._onco_id + 1