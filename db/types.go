package db

//go:generate go-enum -f=$GOFILE --marshal --names -nocase

// ENUM(
//
//	 Ignore
//		Warn
//		Error
//
// )
type OnModuleHashMismatch uint
