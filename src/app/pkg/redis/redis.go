package redis

import (
	"app/pkg/setting"
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	log "github.com/shengkehua/xlog4go"
	"time"
)

var Conn *redis.Pool

func RedisDial() (redisConn redis.Conn, err error) {
	redisConn, err = redis.Dial("tcp", setting.RedisSetting.Host)
	if err != nil {
		log.Fatal("redis dial call err: %v", err)
		return nil, err
	}

	if setting.RedisSetting.Password != "" {
		if _, err := redisConn.Do("AUTH", setting.RedisSetting.Password); err != nil {
			redisConn.Close()
			log.Fatal("redis dial call err: %v", err)
			return nil, err
		}
	}
	return redisConn, nil
}

func RedisTestOnBorrow(redisConn redis.Conn, t time.Time) error {
	_, err := redisConn.Do("PING")
	if err != nil {
		log.Fatal("redis test on borrow err: %v", err)
	}
	return err
}

func Setup() {
	Conn = &redis.Pool{
		MaxIdle:      setting.RedisSetting.MaxIdle,
		MaxActive:    setting.RedisSetting.MaxActive,
		IdleTimeout:  setting.RedisSetting.IdleTimeout,
		Dial:         RedisDial,
		TestOnBorrow: RedisTestOnBorrow,
	}
}

func Set(key string, data interface{}) error {
	conn := Conn.Get()
	defer conn.Close()

	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		return err
	}

	return nil
}

// time > 0 will expire
func Setex(key string, data interface{}, time int) error {
	conn := Conn.Get()
	defer conn.Close()

	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = conn.Do("SETEX", key, time, value)
	if err != nil {
		return err
	}

	return nil
}

/*
* setnx:
*   set if not exist
*   using set to extend setnx
*   if time > 0, will expire, else not expire
 */
func Setnx(key string, data interface{}, time int) (success bool, err error) {
	conn := Conn.Get()
	defer conn.Close()

	value, err := json.Marshal(data)
	if err != nil {
		return false, err
	}

	if time > 0 {
		success, err = redis.Bool(conn.Do("SET", key, value, "EX", time, "NX"))
		if err != nil {
			return false, err
		}
	} else {
		success, err = redis.Bool(conn.Do("SETNX", key, value))
		if err != nil {
			return false, err
		}
	}
	return success, nil
}

func Exists(key string) (exists bool, err error) {
	conn := Conn.Get()
	defer conn.Close()

	exists, err = redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return false, err
	}

	return exists, nil
}

func Delete(key string) (bool, error) {
	conn := Conn.Get()
	defer conn.Close()

	return redis.Bool(conn.Do("DEL", key))
}
