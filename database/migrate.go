package database

import (
	"log"

	"github.com/vsouza/go-kafka/example/models"
	"gorm.io/gorm"
)

func Migrate(db *gorm.DB) {

	err := db.AutoMigrate(
		&models.Message{},
	)

	if err != nil {
		log.Fatal("Migration failed:", err)
	}

	log.Println("Database migrated successfully")
}
