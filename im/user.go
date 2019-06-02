/**
 * Copyright (c) 2014-2015, GoBelieve     
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)
import "time"
import log "github.com/golang/glog"
import "github.com/gomodule/redigo/redis"
import "errors"

func GetSyncKey(appid int64, uid int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	origin, err := redis.Int64(conn.Do("HGET", key, "sync_key"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func GetGroupSyncKey(appid int64, uid int64, group_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	origin, err := redis.Int64(conn.Do("HGET", key, field))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func SaveSyncKey(appid int64, uid int64, sync_key int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	_, err := conn.Do("HSET", key, "sync_key", sync_key)
	if err != nil {
		log.Warning("hset error:", err)
	}
}

func SaveGroupSyncKey(appid int64, uid int64, group_id int64, sync_key int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	_, err := conn.Do("HSET", key, field, sync_key)
	if err != nil {
		log.Warning("hset error:", err)
	}	
}


func GetUserForbidden(appid int64, uid int64) (int, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	forbidden, err := redis.Int(conn.Do("HGET", key, "forbidden"))
	if err != nil {
		log.Info("hget error:", err)
		return 0,  err
	}

	return forbidden, nil
}

func encodeAuthInfo(user_id int64, forbidden int, notification_on bool) ([]byte, error) {
	buf := new(bytes.Buffer)
	uid := int64(user_id)
	err1 := binary.Write(buf, binary.BigEndian, uid)
	if err1 != nil{
		return nil, err1
	}

	err2 := binary.Write(buf, binary.BigEndian, forbidden == 1)
	if err2 != nil{
		return nil, err2
	}

	err3 := binary.Write(buf, binary.BigEndian, notification_on)
	if err3 != nil{
		return nil, err3
	}
	//fmt.Printf("% x", buf.Bytes())
	return buf.Bytes(), nil
}

func decodeAuthInfo(r []byte) (int64, int, bool, error) {
	switch len(r) {
	case 0:
		return 0, 0, false, errors.New("token non exists")
	case 10:
		buffer := bytes.NewBuffer(r)
		var user_id int64
		var fobidden bool
		var notification_on bool
		var b2i = map[bool]int{false: 0, true: 1}
		binary.Read(buffer, binary.BigEndian, &user_id)
		binary.Read(buffer, binary.BigEndian, &fobidden)
		binary.Read(buffer, binary.BigEndian, &notification_on)
		return user_id, b2i[fobidden], notification_on, nil
	default:
		return 0, 0, false, errors.New("token data error")
	}

}


func LoadUserAccessToken(token string) (int64, int64, int, bool, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := "im_access_token"

	var uid int64
	var appid int64 = 1
	var notification_on bool
	var forbidden int

	tokenHex, err := hex.DecodeString(token)
	if err != nil {
		// handle error
		log.Warning("token string error:", err)
		return 0, 0, 0, false,  errors.New("token format error")
	}

	//exists, err := redis.Bool(conn.Do("EXISTS", key))
	//if err != nil {
	//	return 0, 0, 0, false, err
	//}
	//if !exists {
	//	return 0, 0, 0, false,  errors.New("token non exists")
	//}

	reply, err := redis.Bytes(conn.Do("HGET", key, tokenHex))

	if err != nil {
		log.Info("hget error:", err)
		return 0, 0, 0, false, err
	}

	uid, forbidden, notification_on, err =  decodeAuthInfo(reply)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, 0, false, err
	}

	//_, err = redis.Scan(reply, &uid, &appid, &notification_on, &forbidden)
	//if err != nil {
	//	log.Warning("scan error:", err)
	//	return 0, 0, 0, false, err
	//}
	
	return appid, uid, forbidden, notification_on, nil

}

func CountUser(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("statistics_users_%d", appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func CountDAU(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	now := time.Now()
	date := fmt.Sprintf("%d_%d_%d", now.Year(), int(now.Month()), now.Day())
	key := fmt.Sprintf("statistics_dau_%s_%d", date, appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	_, err := conn.Do("HSET", key, "unread", count)
	if err != nil {
		log.Info("hset err:", err)
	}
}
