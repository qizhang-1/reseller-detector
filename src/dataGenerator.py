import string
import random
import socket
import struct
import csv


class dataGenerator(object):
    def __init__(self, n_records, filename, header, random_percent):
        self.n_records = n_records
        self.filename = filename
        self.header = header
        self.random_percent = random_percent

    def _random_number_generator(self, size=1, chars=string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def _random_alpha_numeric_generator(self, size=1, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def generate_user_id(self):
        return self._random_alpha_numeric_generator(size=8)

    def generate_order_number(self):
        return 'ORDER-' + self._random_alpha_numeric_generator(size=10)

    @staticmethod
    def generate_sku(self, is_random=True):
        if is_random:
            return self._random_alpha_numeric_generator(size=7)
        else:
            return "ABC0001"

    def generate_cellphone(self, is_random=True):
        if is_random:
            return "("+  random.choice("23456789") + self._random_number_generator(size=2) + ")" \
                   + self._random_number_generator(size=3) + "-" + self._random_number_generator(size=4)
        else:
            return "(716)238-195" + self._random_number_generator(size=1)

    def generate_ipv4(self, is_random=True):
        def random_ipv4_generator():
            return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))

        if is_random:
            return random_ipv4_generator()
        else:
            return "0.0.0." + self._random_number_generator(size=1)

    def generate_env0(self, is_random=True):
        def random_mac_address_generator():
            return "%02x:%02x:%02x:%02x:%02x:%02x" % (
                random.randint(0, 255),
                random.randint(0, 255),
                random.randint(0, 255),
                random.randint(0, 255),
                random.randint(0, 255),
                random.randint(0, 255)
            )
        if is_random:
            return random_mac_address_generator()
        else:
            return "00:12:34:56:78:9" + self._random_number_generator(size=1)

    def generate_credit_card(self, is_random=True):
        if is_random:
            return random.choice("123456789") + self._random_number_generator(size=15)
        else:
            return "387175999284824" + self._random_number_generator(size=1)

    def generate_csv_data_piece(self):
        user_id = self.generate_user_id()
        order_number = self.generate_order_number()

        is_random = True if random.random() < self.random_percent else False
        cell = self.generate_cellphone(is_random)
        is_random = True if random.random() < self.random_percent else False
        ipv4 = self.generate_ipv4(is_random)
        is_random = True if random.random() < self.random_percent else False
        en0 = self.generate_env0(is_random)
        is_random = True if random.random() < self.random_percent else False
        credit_card = self.generate_credit_card(is_random)
        data_source = list()
        data_source.append([user_id, order_number, cell, ipv4, en0, credit_card])
        return data_source

    def generate_csv_data_file(self, filename):
        file_handle = open(filename, 'w')
        writer = csv.writer(file_handle, delimiter=',')
        writer.writerow(self.header)

        for i in range(self.n_records):
            data_source = self.generate_csv_data_piece()
            writer = csv.writer(file_handle, delimiter=',')
            writer.writerows(data_source)
        file_handle.close()


if __name__ == '__main__':
    n_records = 5000
    filename = 'transaction_' + str(n_records) + '.dat'
    header = ['user_id', 'order_number', 'cell', 'ipv4', 'en0', 'credit_card']
    data_info = dataGenerator(n_records=n_records, filename=filename, header=header, random_percent=0.98)
    data_info.generate_csv_data_file(data_info.filename)


