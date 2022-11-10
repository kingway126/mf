package mf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cast"
	"gorm.io/gorm"
	"reflect"
	"time"
)

type ModelFunc struct {
	MysqlCient  *gorm.DB              // 数据库链接
	UseCache    bool                  // 是否使用缓存 true 自动走redis
	RedisClient *redis.Client         // 数据库链接
	RedisPrefix string                // redis 缓存 前缀
	Expire      time.Duration         // redis 缓存 过期间隔
	LinkMap     map[string]LinkFinder // redis 其他字段关联表id的查询方法
}

func NewMf(db *gorm.DB) *ModelFunc {
	return &ModelFunc{MysqlCient: db}
}

type LinkFinder interface {
	Find(ctx context.Context, db *gorm.DB, field string) (id uint64, err error)
	FieldValue(model interface{}) (fieldValue string)
}

/**
方法列表
	Create							// 新增一条记录
	UpdateById						// 使用id更新记录,空字段不处理
	SaveById						// 使用id更新记录
	FirstById						// 使用id查询记录
	FirstByLink 					// 使用link查询记录
	FirstByLinkSD 					// 使用link查询记录，并剔除被软删的记录
	FirstByIdSD						// 使用id查询记录，并剔除被软删的记录
	DeleteById						// 使用id删除记录
	SoftDeleteById					// 使用id软删记录
参数说明
	model 参数必须是指针类型的模型
钩子
	MfAfterUpdateById(ctx context.Context, db *gorm.Db, rdc *redis.Client)			// 在 UpdateById 方法执行之后 执行
	MfAfterSaveById(ctx context.Context, db *gorm.Db, rdc *redis.Client)			// 在 SaveById 方法执行之后 执行
	MfAfterDeleteById(ctx context.Context, db *gorm.Db, rdc *redis.Client)			// 在 DeleteById 方法执行之后 执行
	MfAfterSoftDeleteById(ctx context.Context, db *gorm.Db, rdc *redis.Client)		// 在 SoftDeleteById 方法执行之后 执行
逻辑说明
	使用缓存时，更新数据，会清理调对应的缓存。查询时才会创建对应的缓存
*/

func (c *ModelFunc) Create(ctx context.Context, model interface{}) error {
	return c.MysqlCient.WithContext(ctx).Create(model).Error
}

func (c *ModelFunc) UpdateById(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.updateByIdR(ctx, model, id)
	} else {
		err = c.updateByIdM(ctx, model, id)
	}

	return c.hook("MfAfterUpdateById", ctx, model)
}

func (c *ModelFunc) SaveById(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.saveByIdR(ctx, model, id)
	} else {
		err = c.saveByIdM(ctx, model, id)
	}

	return c.hook("MfAfterSaveById", ctx, model)
}

func (c *ModelFunc) FirstById(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.firstByIdR(ctx, model, id)
	} else {
		err = c.firstByIdM(ctx, model, id)
	}
	return
}

func (c *ModelFunc) FirstByLink(ctx context.Context, linkType string, model interface{}, field string) (err error) {
	finder, exist := c.LinkMap[linkType]
	if !exist {
		return errors.New("不存在指定的 linkType")
	}
	id, _ := c.getLink(ctx, linkType, field)
	if cast.ToUint64(id) == 0 {
		idInt, err := finder.Find(ctx, c.MysqlCient, field)
		if err != nil {
			return err
		}

		if idInt > 0 {
			if err = c.createLink(ctx, idInt, linkType, field); err != nil {
				return err
			}

			id = cast.ToString(idInt)
		}
	}
	if cast.ToUint64(id) > 0 {
		err = c.FirstById(ctx, model, cast.ToUint64(id))
	}
	return
}

func (c *ModelFunc) FirstByIdSD(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.firstByIdFilterSoftDelR(ctx, model, id)
	} else {
		err = c.firstByIdFilterSoftDelM(ctx, model, id)
	}
	return
}

func (c *ModelFunc) FirstByLinkSD(ctx context.Context, linkType string, model interface{}, field string) (err error) {
	finder, exist := c.LinkMap[linkType]
	if !exist {
		return errors.New("不存在指定的 linkType")
	}
	id, _ := c.getLink(ctx, linkType, field)
	if cast.ToUint64(id) == 0 {
		idInt, err := finder.Find(ctx, c.MysqlCient, field)
		if err != nil {
			return err
		}

		if idInt > 0 {
			if err = c.createLink(ctx, idInt, linkType, field); err != nil {
				return err
			}

			id = cast.ToString(idInt)
		}
	}
	if cast.ToUint64(id) > 0 {
		err = c.FirstById(ctx, model, cast.ToUint64(id))
	}
	return
}

func (c *ModelFunc) DeleteById(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.deleteByIdR(ctx, model, id)
	} else {
		err = c.deleteByIdM(ctx, model, id)
	}
	return c.hook("MfAfterDeleteById", ctx, model)
}

func (c *ModelFunc) SoftDeleteById(ctx context.Context, model interface{}, id uint64) (err error) {
	if c.UseCache {
		err = c.softDeleteByIdR(ctx, model, id)
	} else {
		err = c.softDeleteByIdM(ctx, model, id)
	}
	return c.hook("MfAfterSoftDeleteById", ctx, model)
}

func (c *ModelFunc) cacheKey(id uint64) string {
	return c.RedisPrefix + "id:" + cast.ToString(id)
}

func (c *ModelFunc) deleteCache(ctx context.Context, id uint64) error {
	return c.RedisClient.Del(ctx, c.cacheKey(id)).Err()
}

func (c *ModelFunc) updateCache(ctx context.Context, model interface{}, id uint64) error {
	marshalData, _ := json.Marshal(model)

	return c.RedisClient.Set(ctx, c.cacheKey(id), string(marshalData), c.Expire).Err()
}

func (c *ModelFunc) getCache(ctx context.Context, model interface{}, id uint64) error {
	res, err := c.RedisClient.Get(ctx, c.cacheKey(id)).Result()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(res), model)
}

func (c *ModelFunc) updateByIdM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Where("id = ?", id).Updates(model).Error
}

func (c *ModelFunc) updateByIdR(ctx context.Context, model interface{}, id uint64) error {
	// 更新
	if err := c.updateByIdM(ctx, model, id); err != nil {
		return err
	}

	// 清除缓存
	if err := c.deleteCache(ctx, id); err != nil {
		return err
	}

	// 清除link缓存
	for linkType, linkFunc := range c.LinkMap {
		c.delLink(ctx, linkType, linkFunc.FieldValue(model))
	}

	// 返回
	return nil
}

func (c *ModelFunc) saveByIdM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Where("id = ?", id).Save(model).Error
}

func (c *ModelFunc) saveByIdR(ctx context.Context, model interface{}, id uint64) error {
	// 更新
	if err := c.saveByIdM(ctx, model, id); err != nil {
		return err
	}

	// 清除缓存
	if err := c.deleteCache(ctx, id); err != nil {
		return err
	}

	// 清除link缓存
	for linkType, linkFunc := range c.LinkMap {
		c.delLink(ctx, linkType, linkFunc.FieldValue(model))
	}

	// 返回
	return nil
}

func (c *ModelFunc) firstByIdM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Where("id = ?", id).First(model).Error
}

func (c *ModelFunc) firstByIdR(ctx context.Context, model interface{}, id uint64) error {
	if err := c.getCache(ctx, model, id); err != nil && !ErrIsRedisNil(err) {
		return err
	} else if ErrIsRedisNil(err) {
		if err = c.firstByIdM(ctx, model, id); err != nil {
			return err
		}

		if err = c.updateCache(ctx, model, id); err != nil {
			return err
		}
	}

	return nil
}

func (c *ModelFunc) firstByIdFilterSoftDelM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Where("id = ? AND deleted_at = ?", id, time.Time{}).First(model).Error
}

func (c *ModelFunc) firstByIdFilterSoftDelR(ctx context.Context, model interface{}, id uint64) error {
	if err := c.getCache(ctx, model, id); err != nil && !ErrIsRedisNil(err) {
		return err
	} else if ErrIsRedisNil(err) {
		if err = c.firstByIdFilterSoftDelM(ctx, model, id); err != nil {
			return err
		}

		if err = c.updateCache(ctx, model, id); err != nil {
			return err
		}
	}

	return nil
}

func (c *ModelFunc) deleteByIdM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Where("id = ?", id).Delete(model).Error
}

func (c *ModelFunc) deleteByIdR(ctx context.Context, model interface{}, id uint64) error {
	if err := c.deleteByIdM(ctx, model, id); err != nil {
		return err
	}

	if err := c.deleteCache(ctx, id); err != nil {
		return err
	}

	// 清除link缓存
	for linkType, linkFunc := range c.LinkMap {
		c.delLink(ctx, linkType, linkFunc.FieldValue(model))
	}

	return nil
}

func (c *ModelFunc) softDeleteByIdM(ctx context.Context, model interface{}, id uint64) error {
	return c.MysqlCient.WithContext(ctx).Model(model).Where("id = ?", id).Updates(map[string]interface{}{"deleted_at": GetNowTime()}).Error
}

func (c *ModelFunc) softDeleteByIdR(ctx context.Context, model interface{}, id uint64) error {
	if err := c.softDeleteByIdM(ctx, model, id); err != nil {
		return err
	}

	if err := c.deleteCache(ctx, id); err != nil {
		return err
	}

	// 清除link缓存
	for linkType, linkFunc := range c.LinkMap {
		c.delLink(ctx, linkType, linkFunc.FieldValue(model))
	}

	return nil
}

func (c *ModelFunc) linkKey(linkType, field string) string {
	return fmt.Sprintf("%s%s:%s", c.RedisPrefix, linkType, field)
}

func (c *ModelFunc) getLink(ctx context.Context, linkType, field string) (string, error) {
	if field == "" {
		return "", errors.New("getLink 缺少参数 field")
	}
	return c.RedisClient.WithContext(ctx).Get(ctx, c.linkKey(linkType, field)).Result()
}

func (c *ModelFunc) createLink(ctx context.Context, id uint64, linkType, field string) error {
	if id == 0 {
		return errors.New("createLink 缺少参数 id")
	} else if field == "" {
		return errors.New("createLink 缺少参数 field")
	}
	return c.RedisClient.WithContext(ctx).Set(ctx, c.linkKey(linkType, field), id, time.Hour*24*7).Err()
}

func (c *ModelFunc) delLink(ctx context.Context, linkType, field string) error {
	if field == "" {
		return errors.New("delLink 缺少参数 field")
	}
	return c.RedisClient.WithContext(ctx).Del(ctx, c.linkKey(linkType, field)).Err()
}

func (c *ModelFunc) hook(hookMethod string, ctx context.Context, model interface{}) error {
	a := reflect.ValueOf(model)
	m := a.MethodByName(hookMethod)
	if !m.IsValid() {
		return nil
	}
	params := make([]reflect.Value, 3)
	params[0] = reflect.ValueOf(ctx)
	params[1] = reflect.ValueOf(c.MysqlCient)
	params[2] = reflect.ValueOf(c.RedisClient)

	values := m.Call(params)
	switch values[0].Interface().(type) {
	case error:
		return values[0].Interface().(error)
	default:
		return nil
	}
}

func ErrIsGormNil(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func ErrIsRedisNil(err error) bool {
	return errors.Is(err, redis.Nil)
}

// 返回当前时间
func GetNowTime() time.Time {
	cstZone := time.FixedZone("CST", 8*3600) // 东八
	now := time.Now().In(cstZone)
	return now
}
