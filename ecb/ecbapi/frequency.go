package ecbapi

type Frequency string

func (e Frequency) String() string {
	return string(e)
}

const (
	Daily   Frequency = "D"
	Monthly Frequency = "M"
)
