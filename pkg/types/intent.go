package types

type Intent struct {
	name string
	data []byte
}

func NewIntent(name string, data []byte) *Intent {
	return &Intent{
		name: name,
		data: data,
	}
}

func (i *Intent) Name() string {
	return i.name
}

func (i *Intent) Data() []byte {
	return i.data
}
