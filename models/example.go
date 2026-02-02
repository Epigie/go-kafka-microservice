package models

type Message struct {
	ID      uint   `gorm:"primaryKey;autoIncrement"`
	Content string `gorm:"type:text;not null"`
	Status  string `gorm:"type:varchar(50);not null"`
}