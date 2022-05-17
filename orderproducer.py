from faker.providers import BaseProvider
import random
import time
import datetime

orderOwner = ['Deja Brew', 'Jurassic Pork', 'Lawn & Order', 'Pita Pan', 'Bread Pitt', 'Indiana Jeans', 'Thai Tanic']
SeedValues = [10.0, 20.1, 20.2, 12.1, 25.1, 25.1, 27.5]
UpProb = [0.5, 0.6, 0.7, 0.8, 0.9, 0.4, 0.3]
ShuffleProb = 0.2
ChangeAmount = 0.8

class OrderProvider(BaseProvider):

    def order_owner(self):
        return random.choice(orderOwner)
    
    def order_id(self, orderowner):
        indexOrder = orderOwner.index(orderowner)
        return indexOrder

    def order_value(self, orderowner):
        indexOrder = orderOwner.index(orderowner)
        currentval = SeedValues[indexOrder]
        goesup = 1
        if random.random() > UpProb[indexOrder]:
            goesup = -1
        nextval = currentval + random.random() * ChangeAmount * goesup
        SeedValues[indexOrder] = nextval
        
        return int(nextval*1000)

    def reshuffle_probs(self, orderowner):
        indexOrder = orderOwner.index(orderowner)
        UpProb[indexOrder]=random.random()
        
    
    def produce_msg(self):
        orderowner =self.order_owner()
        ts = time.time()
        if random.random() > ShuffleProb:
            self.reshuffle_probs(orderowner)
        message = {
            'order_id': self.order_id(orderowner),
            'order_owner': orderowner,
            'order_value': self.order_value(orderowner),
            'timestamp': int(ts*1000)
        }
        key = {'order_id': self.order_id(orderowner)}
        return message, key