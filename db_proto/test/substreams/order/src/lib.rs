mod pb;

use std::i64;
use pb::test as model;

use substreams_solana::pb::sf::solana::r#type::v1::Block;
use crate::pb::test::relations::OrderExtension;

#[substreams::handlers::map]
fn map_output(block: Block) -> model::relations::Output {
    let mut entities = vec![];


    let long_string = "a".repeat(255);

    let byte_vector: Vec<u8> = (1..=255).collect();
    
    if block.slot % 100 != 0 {
        entities.push(model::relations::Entity {
            entity: Some(model::relations::entity::Entity::TypesTest {
                0: model::relations::TypesTest {
                    id: block.slot,
                    int32_field: i32::MAX,
                    int64_field: i64::MAX,
                    uint32_field: u32::MAX,
                    uint64_field: u64::MAX,
                    sint32_field: i32::MAX,
                    sint64_field: i64::MAX,
                    fixed32_field: u32::MAX,
                    fixed64_field: u64::MAX,
                    sfixed32_field: i32::MAX,
                    float_field: f32::MAX,
                    double_field: f64::MAX,
                    string_field: long_string,
                    bytes_field: byte_vector,
                    timestamp_field: Some(prost_types::Timestamp::default()),
                    bool_field: true,
                    // Add other fields if there are more in your `TypesTest` message definition
                    sfixed64_field: i64::MAX,
                },
            }),
        });
    }

    entities.push(model::relations::Entity {
        entity: Some(model::relations::entity::Entity::Customer {
            0: model::relations::Customer {
                name: format!("customer.name.{}", block.slot),
                customer_id: format!("customer.id.{}", block.slot),
            },
        }),
    });

    entities.push(model::relations::Entity {
        entity: Some(model::relations::entity::Entity::Item {
            0: model::relations::Item {
                item_id: format!("item.id.{}", block.slot),
                name: format!("item.name.{}", block.slot),
                price: 99.99,
            },
        }),
    });

    entities.push(model::relations::Entity {
        entity: Some(model::relations::entity::Entity::Order {
            0: model::relations::Order {
                order_id: format!("order.id.{}", block.slot),
                customer_ref_id: format!("customer.id.{}", block.slot),
                items: vec![
                    model::relations::OrderItem {
                        item_id: format!("item.id.{}", block.slot),
                        quantity: 10,
                    },
                    // model::relations::OrderItem { item_id: format!("item.id.{}", block.slot+1), quantity: 20 },
                ],
                extension: Some(OrderExtension{ description: "desc".to_string() }),
            },
        }),
    });

    model::relations::Output { entities }
}
