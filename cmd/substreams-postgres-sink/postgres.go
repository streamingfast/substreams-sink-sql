package main

//func applyDatabaseChanges(loader *PostgresLoader, databaseChanges *database.DatabaseChanges, schemaName string) error {
//	for _, change := range databaseChanges.TableChanges {
//		_, tableExists := loader.tableRegistry[[2]string{loader.schema, change.Table}]
//		if !tableExists {
//			continue
//		}
//
//		id := change.Pk
//		changes := map[string]string{}
//		for _, field := range change.Fields {
//			changes[field.Name] = field.NewValue
//		}
//
//		switch change.Operation {
//		case database.TableChange_CREATE:
//			err := loader.Insert(schemaName, change.Table, id, changes)
//			if err != nil {
//				return fmt.Errorf("loader insert: %w", err)
//			}
//		case database.TableChange_UPDATE:
//			err := loader.Update(schemaName, change.Table, id, changes)
//			if err != nil {
//				return fmt.Errorf("loader update: %w", err)
//			}
//		case database.TableChange_DELETE:
//			err := loader.Delete(schemaName, change.Table, id)
//			if err != nil {
//				return fmt.Errorf("loader delete: %w", err)
//			}
//		default:
//			//case database.TableChange_UNSET:
//		}
//	}
//	return nil
//}
