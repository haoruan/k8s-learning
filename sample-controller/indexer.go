package main

type Indexer interface {
	Store
}

type cache struct {
	items   map[string]interface{}
	keyFunc KeyFunc
}

func NewCache(keyFunc KeyFunc) Indexer {
	return &cache{keyFunc: keyFunc}
}

func (c *cache) Add(obj interface{}) error {
	return c.Update((obj))
}

func (c *cache) Update(obj interface{}) error {
	key, _ := c.keyFunc(obj)
	c.items[key] = obj

	return nil
}

// Delete deletes the given object from the accumulator associated with the given object's key
func (c *cache) Delete(obj interface{}) error {
	key, _ := c.keyFunc(obj)
	delete(c.items, key)

	return nil
}

// Replace will delete the contents of the store, using instead the
// given list. Store takes ownership of the list, you should not reference
// it after calling this function.
func (c *cache) Replace(list []interface{}) error {
	for _, item := range list {
		c.Update(item)
	}

	return nil
}

func (c *cache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, _ := c.keyFunc(obj)
	item, exists = c.items[key]

	return item, exists, nil
}
