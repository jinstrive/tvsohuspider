import redis
import md5

class RedisOper(object):
    def __init__(self):
        pool = redis.ConnectionPool(host='localhost',port=6379,db=0)
        self._redis = redis.Redis(connection_pool=pool)
    
    def sadd(self,key,value):
        return self._redis.sadd(key,value)
    
    def scard(self,key):
        return self._redis.scard(key)
    
    def spop(self,key):
        return self._redis.spop(key)
    
    def sismember(self,key,value):
        return self._redis.sismember(key, value)
    
    def smembers(self,key):
        return self._redis.smembers(key)
    
    def srem(self,key,values):
        return self._redis.srem(key,values)

class SpiderRedisOper(RedisOper):
    
    def __init__(self):
        RedisOper.__init__(self)
        self._isunique_key = "unique_url"
        self._url_list_g = "url_list_for_generate"
        self._url_list_a = "url_list_for_analyze"

    def turn_scrkey(self, value):
        m = md5.new()
        m.update(value)
        scr_key = long(m.hexdigest(), 16)
        return scr_key

    def insertUrl(self,value):
        scr_key = self.turn_scrkey(value)
        if self.sismember(self._isunique_key, scr_key):
            return (0,0)
        ret = self.sadd(self._isunique_key, scr_key)
        exist_num = self.scard(self._isunique_key)
        self.sadd(self._url_list_g,value)
        self.sadd(self._url_list_a,value)
        return ret,exist_num
    
    def pop_for_generate(self):
        url = self.spop(self._url_list_g)
        if not url:
            return None
        return url
    
    def pop_for_analyze(self):
        url = self.spop(self._url_list_a)
        if not url:
            return None
        return url
    
    def show_count_a(self):
        return self.scard(self._url_list_a)
    
    def show_count_g(self):
        return self.scard(self._url_list_g)
    
    def show_uniques(self):
        return self.smembers(self._isunique_key)
    
    def clear_uniques(self,key,values):
        return self.srem(key,values)
        
    
if __name__ == "__main__":
    myredis = SpiderRedisOper()
    print myredis.clear_uniques(myredis._isunique_key,myredis.turn_scrkey("http://tv.sohu.com/s2014/mhkj/"))
    print myredis.insertUrl("http://tv.sohu.com/s2014/mhkj/")
    print myredis.pop_for_analyze()
    print myredis.pop_for_generate()
    print myredis.show_uniques()