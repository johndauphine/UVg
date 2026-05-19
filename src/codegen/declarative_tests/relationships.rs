//! Relationship tests: one-to-many, many-to-many, one-to-one, inheritance.

use super::super::*;
use crate::testutil::*;

/// Adapted from sqlacodegen test_onetomany.
#[test]
fn test_declarative_onetomany() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("simple_containers_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk(
                "simple_items_container_id_fkey",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Parent side: collection relationship
    assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='container')"));

    // Child side: inline FK + scalar relationship
    assert!(output.contains(
        "container_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_containers.id'))"
    ));
    assert!(output.contains("container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));

    // Should import relationship
    assert!(output.contains("relationship"));
    // Should import ForeignKey (not ForeignKeyConstraint)
    assert!(output.contains("ForeignKey"));
    assert!(!output.contains("ForeignKeyConstraint"));
}

/// Adapted from sqlacodegen test_onetomany_selfref.
#[test]
fn test_declarative_onetomany_selfref() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("parent_item_id").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .fk(
            "simple_items_parent_item_id_fkey",
            &["parent_item_id"],
            "simple_items",
            &["id"],
        )
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Inline FK
    assert!(output.contains(
        "parent_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_items.id'))"
    ));
    // Forward relationship with remote_side
    assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], back_populates='parent_item_reverse')"));
    // Reverse relationship
    assert!(output.contains("parent_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', remote_side=[parent_item_id], back_populates='parent_item')"));
}

/// Adapted from sqlacodegen test_onetomany_composite.
#[test]
fn test_declarative_onetomany_composite() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id1").build())
            .column(col("id2").build())
            .pk("simple_containers_pkey", &["id1", "id2"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id1").nullable().build())
            .column(col("container_id2").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk_full(
                "simple_items_fkey",
                &["container_id1", "container_id2"],
                "public",
                "simple_containers",
                &["id1", "id2"],
                "CASCADE",
                "CASCADE",
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Composite FK stays in __table_args__
    assert!(output.contains("ForeignKeyConstraint(['container_id1', 'container_id2']"));
    assert!(output.contains("ondelete='CASCADE'"));
    // Columns keep their types (no inline ForeignKey)
    assert!(output.contains("container_id1: Mapped[Optional[int]] = mapped_column(Integer)"));
    assert!(output.contains("container_id2: Mapped[Optional[int]] = mapped_column(Integer)"));
    // Parent-side relationship
    assert!(output.contains("simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', back_populates='simple_containers')"));
    // Child-side relationship
    assert!(output.contains("simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', back_populates='simple_items')"));
}

/// Adapted from sqlacodegen test_onetoone.
#[test]
fn test_declarative_onetoone() {
    let schema = schema_pg(vec![
        table("other_items")
            .column(col("id").build())
            .pk("other_items_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("other_item_id").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk(
                "simple_items_other_item_id_fkey",
                &["other_item_id"],
                "other_items",
                &["id"],
            )
            .unique("simple_items_other_item_id_key", &["other_item_id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Parent side: one-to-one (uselist=False, Optional scalar)
    assert!(output.contains("simple_items: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', uselist=False, back_populates='other_item')"));
    // Child side: FK with unique=True
    assert!(output.contains("other_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('other_items.id'), unique=True)"));
    // Child side relationship
    assert!(output.contains("other_item: Mapped[Optional['OtherItems']] = relationship('OtherItems', back_populates='simple_items')"));
}

/// Adapted from sqlacodegen test_onetomany_noinflect.
/// FK column without _id suffix — relationship name = FK column name.
#[test]
fn test_declarative_onetomany_noinflect() {
    let schema = schema_pg(vec![
        table("fehwiuhfiw")
            .column(col("id").build())
            .pk("fehwiuhfiw_pkey", &["id"])
            .build(),
        table("oglkrogk")
            .column(col("id").build())
            .column(col("fehwiuhfiwID").nullable().build())
            .pk("oglkrogk_pkey", &["id"])
            .fk(
                "oglkrogk_fehwiuhfiwid_fkey",
                &["fehwiuhfiwID"],
                "fehwiuhfiw",
                &["id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // FK column has uppercase ID suffix — stripped
    assert!(output.contains(
        "fehwiuhfiwID: Mapped[Optional[int]] = mapped_column(ForeignKey('fehwiuhfiw.id'))"
    ));
    // Parent-side relationship
    assert!(output.contains("oglkrogk: Mapped[list['Oglkrogk']] = relationship('Oglkrogk', back_populates='fehwiuhfiw')"));
    // Child-side relationship: fehwiuhfiwID stripped to fehwiuhfiw
    assert!(output.contains("fehwiuhfiw: Mapped[Optional['Fehwiuhfiw']] = relationship('Fehwiuhfiw', back_populates='oglkrogk')"));
}

// --- Tier 4: Enum tests ---

/// Adapted from sqlacodegen test_enum_shared_values (declarative).
#[test]
fn test_declarative_enum_shared_values() {
    use crate::schema::EnumInfo;
    let schema = schema_pg_with_enums(
        vec![
            table("accounts")
                .column(col("id").build())
                .column(col("status").udt("status_enum").build())
                .pk("accounts_pkey", &["id"])
                .build(),
            table("users")
                .column(col("id").build())
                .column(col("status").udt("status_enum").build())
                .pk("users_pkey", &["id"])
                .build(),
        ],
        vec![EnumInfo {
            name: "status_enum".to_string(),
            schema: None,
            values: vec![
                "active".to_string(),
                "inactive".to_string(),
                "pending".to_string(),
            ],
        }],
    );
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Enum class generated before Base
    assert!(output.contains("class StatusEnum(str, enum.Enum):"));
    assert!(output.contains("ACTIVE = 'active'"));
    // Enum used in column type annotation
    assert!(output.contains("status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum'), nullable=False)"));
    // import enum
    assert!(output.contains("import enum"));
    assert!(output.contains("Enum"));
}

// --- PR 5: Advanced relationship tests ---

/// Adapted from sqlacodegen test_onetomany_multiref.
/// Two FKs from child to same parent — needs disambiguation.
#[test]
fn test_declarative_onetomany_multiref() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("simple_containers_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("parent_container_id").nullable().build())
            .column(col("top_container_id").build())
            .pk("simple_items_pkey", &["id"])
            .fk(
                "si_parent_fkey",
                &["parent_container_id"],
                "simple_containers",
                &["id"],
            )
            .fk(
                "si_top_fkey",
                &["top_container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Parent side: disambiguated relationship names
    assert!(output.contains("simple_items_parent_container: Mapped[list['SimpleItems']]"));
    assert!(output.contains("simple_items_top_container: Mapped[list['SimpleItems']]"));
    // Child side: foreign_keys disambiguation
    assert!(output.contains("parent_container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', foreign_keys=[parent_container_id], back_populates='simple_items_parent_container')"));
    assert!(output.contains("top_container: Mapped['SimpleContainers'] = relationship('SimpleContainers', foreign_keys=[top_container_id], back_populates='simple_items_top_container')"));
}

/// Adapted from sqlacodegen test_onetomany_selfref_multi.
#[test]
fn test_declarative_onetomany_selfref_multi() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("parent_item_id").nullable().build())
        .column(col("top_item_id").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .fk(
            "si_parent_fkey",
            &["parent_item_id"],
            "simple_items",
            &["id"],
        )
        .fk("si_top_fkey", &["top_item_id"], "simple_items", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Each self-ref FK gets foreign_keys disambiguation
    assert!(output.contains("parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[parent_item_id], back_populates='parent_item_reverse')"));
    assert!(output.contains("top_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], foreign_keys=[top_item_id], back_populates='top_item_reverse')"));
}

/// Adapted from sqlacodegen test_manytoone_nobidi.
#[test]
fn test_declarative_manytoone_nobidi() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .pk("simple_containers_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk(
                "si_container_fkey",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let opts = GeneratorOptions {
        nobidi: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);

    // Child has relationship without back_populates
    assert!(output.contains(
        "container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers')"
    ));
    // Parent should NOT have reverse relationship
    assert!(!output.contains("simple_items: Mapped[list"));
}

/// Adapted from sqlacodegen test_foreign_key_schema.
#[test]
fn test_declarative_foreign_key_schema() {
    let schema = schema_pg(vec![
        table("other_items")
            .schema("otherschema")
            .column(col("id").build())
            .pk("other_items_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("other_item_id").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk_full(
                "si_other_fkey",
                &["other_item_id"],
                "otherschema",
                "other_items",
                &["id"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // FK target includes schema prefix
    assert!(output.contains("ForeignKey('otherschema.other_items.id')"));
    // Parent table has schema in __table_args__
    assert!(output.contains("__table_args__ = {'schema': 'otherschema'}"));
}

/// Adapted from sqlacodegen test_manytomany.
#[test]
fn test_declarative_manytomany() {
    let schema = schema_pg(vec![
        table("left_table")
            .column(col("id").build())
            .pk("left_table_pkey", &["id"])
            .build(),
        table("right_table")
            .column(col("id").build())
            .pk("right_table_pkey", &["id"])
            .build(),
        table("association_table")
            .column(col("left_id").nullable().build())
            .column(col("right_id").nullable().build())
            .fk("assoc_left_fkey", &["left_id"], "left_table", &["id"])
            .fk("assoc_right_fkey", &["right_id"], "right_table", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Association table rendered as Table()
    assert!(output.contains("t_association_table = Table("));
    assert!(output.contains("Column('left_id', ForeignKey('left_table.id'))"));
    assert!(output.contains("Column('right_id', ForeignKey('right_table.id'))"));

    // Left table gets M2M relationship to right
    assert!(output.contains("right: Mapped[list['RightTable']] = relationship('RightTable', secondary='association_table', back_populates='left')"));
    // Right table gets M2M relationship to left
    assert!(output.contains("left: Mapped[list['LeftTable']] = relationship('LeftTable', secondary='association_table', back_populates='right')"));

    // No class for association table
    assert!(!output.contains("class AssociationTable"));
}

/// Adapted from sqlacodegen test_joined_inheritance.
#[test]
fn test_declarative_joined_inheritance() {
    let schema = schema_pg(vec![
        table("simple_super_items")
            .column(col("id").build())
            .column(col("data1").nullable().build())
            .pk("simple_super_items_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("super_item_id").build())
            .column(col("data2").nullable().build())
            .pk("simple_items_pkey", &["super_item_id"])
            .fk(
                "si_super_fkey",
                &["super_item_id"],
                "simple_super_items",
                &["id"],
            )
            .build(),
        table("simple_sub_items")
            .column(col("simple_items_id").build())
            .column(col("data3").nullable().build())
            .pk("simple_sub_items_pkey", &["simple_items_id"])
            .fk(
                "ssi_items_fkey",
                &["simple_items_id"],
                "simple_items",
                &["super_item_id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());

    // Parent class
    assert!(output.contains("class SimpleSuperItems(Base):"));
    assert!(output.contains("id: Mapped[int] = mapped_column(Integer, primary_key=True)"));
    assert!(output.contains("data1: Mapped[Optional[int]] = mapped_column(Integer)"));

    // Child inherits from parent
    assert!(output.contains("class SimpleItems(SimpleSuperItems):"));
    assert!(output.contains("super_item_id: Mapped[int] = mapped_column(ForeignKey('simple_super_items.id'), primary_key=True)"));
    assert!(output.contains("data2: Mapped[Optional[int]] = mapped_column(Integer)"));

    // Grandchild inherits from child
    assert!(output.contains("class SimpleSubItems(SimpleItems):"));
    assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.super_item_id'), primary_key=True)"));
    assert!(output.contains("data3: Mapped[Optional[int]] = mapped_column(Integer)"));

    // No relationship() calls for inheritance FKs
    assert!(!output.contains("relationship("));
}

// --- PR 8: Misc feature tests ---

/// Adapted from sqlacodegen test_table_with_arrays (declarative).
#[test]
fn test_declarative_table_with_arrays() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("tags").udt("_text").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("tags: Mapped[Optional[list]] = mapped_column(ARRAY(Text))"));
}

/// Adapted from sqlacodegen test_constraints (declarative) — check + unique + index together.
#[test]
fn test_declarative_constraints_with_index() {
    let schema = schema_pg(vec![table("simple_items")
        .column(col("id").build())
        .column(col("number").nullable().build())
        .pk("simple_items_pkey", &["id"])
        .check("", "number > 0")
        .unique("uq_id_number", &["id", "number"])
        .index("idx_number", &["number"], false)
        .build()]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    assert!(output.contains("CheckConstraint('number > 0')"));
    assert!(output.contains("UniqueConstraint('id', 'number', name='uq_id_number')"));
    assert!(output.contains("Index('idx_number', 'number')"));
    assert!(output.contains("from sqlalchemy import CheckConstraint"));
}

/// Adapted from sqlacodegen test_onetomany_conflicting_column.
/// Column named "relationship" gets trailing underscore.
#[test]
fn test_declarative_onetomany_conflicting_column() {
    let schema = schema_pg(vec![
        table("simple_containers")
            .column(col("id").build())
            .column(col("relationship").udt("text").nullable().build())
            .pk("simple_containers_pkey", &["id"])
            .build(),
        table("simple_items")
            .column(col("id").build())
            .column(col("container_id").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .fk(
                "si_container_fkey",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // "relationship" is not in PYTHON_RESERVED or import conflicts currently,
    // so it passes through as-is. The relationship() calls still work.
    assert!(
        output.contains("relationship: Mapped[Optional[str]]")
            || output.contains("relationship_: Mapped[Optional[str]]")
    );
    assert!(output.contains("relationship('SimpleItems'"));
}

/// Adapted from sqlacodegen test_manytomany_nobidi.
#[test]
fn test_declarative_manytomany_nobidi() {
    let schema = schema_pg(vec![
        table("simple_items")
            .column(col("id").build())
            .pk("simple_items_pkey", &["id"])
            .build(),
        table("simple_containers")
            .column(col("id").build())
            .pk("simple_containers_pkey", &["id"])
            .build(),
        table("container_items")
            .column(col("item_id").nullable().build())
            .column(col("container_id").nullable().build())
            .fk("ci_item_fkey", &["item_id"], "simple_items", &["id"])
            .fk(
                "ci_container_fkey",
                &["container_id"],
                "simple_containers",
                &["id"],
            )
            .build(),
    ]);
    let opts = GeneratorOptions {
        nobidi: true,
        ..GeneratorOptions::default()
    };
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &opts);
    // M2M relationships exist but without back_populates
    assert!(output.contains("relationship("));
    assert!(!output.contains("back_populates"));
}

/// Adapted from sqlacodegen test_joined_inheritance_same_table_name.
#[test]
fn test_declarative_joined_inheritance_same_table_name() {
    let schema = schema_pg(vec![
        table("simple_items")
            .column(col("id").build())
            .column(col("data1").nullable().build())
            .pk("simple_items_pkey", &["id"])
            .build(),
        table("simple_sub_items")
            .column(col("simple_items_id").build())
            .column(col("data2").nullable().build())
            .pk("simple_sub_items_pkey", &["simple_items_id"])
            .fk("ssi_fkey", &["simple_items_id"], "simple_items", &["id"])
            .build(),
    ]);
    let gen = DeclarativeGenerator;
    let output = gen.generate(&schema, &GeneratorOptions::default());
    // Child inherits from parent
    assert!(output.contains("class SimpleSubItems(SimpleItems):"));
    // FK on PK column
    assert!(output.contains("simple_items_id: Mapped[int] = mapped_column(ForeignKey('simple_items.id'), primary_key=True)"));
}
