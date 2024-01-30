/**
 * @file heap_table.cpp - Implementation of HeapTable class
*/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes),
      file(table_name) {}

void HeapTable::create() { this->file.create(); }

void HeapTable::create_if_not_exists() {
    try {
        this->file.open();
    } catch (DbException &e) {  // DBNoSuchFileError
        this->create();
    }
}

void HeapTable::drop() { this->file.drop(); }

void HeapTable::open() { this->file.open(); }

void HeapTable::close() { this->file.close(); }

Handle HeapTable::insert(const ValueDict *row) {
    ValueDict *validatedRow = validate(row);
    Handle handle = append(validatedRow);
    delete validatedRow;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

Handles *HeapTable::select() {
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles *HeapTable::select(const ValueDict *where) { return nullptr; }

ValueDict *HeapTable::project(Handle handle) {
    return this->project(handle, &this->column_names);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID blockID = handle.first;
    RecordID recordID = handle.second;
    SlottedPage *page = this->file.get(blockID);
    Dbt *data = page->get(recordID);
    ValueDict *row = unmarshal(data);

    ValueDict *projectedRow = new ValueDict();
    for (auto const &columnName : *column_names) {
        auto search = row->find(columnName);
        if (search == row->end()) throw DbRelationError("Invalid column name");
        projectedRow->insert({columnName, search->second});
    }
    return projectedRow;
}

ValueDict *HeapTable::validate(const ValueDict *row) {
    std::map<Identifier, ColumnAttribute::DataType> identifierToDataTypeMap;
    for (size_t i = 0; i < this->column_names.size(); i++)
        identifierToDataTypeMap.insert(
            {this->column_names[i],
             this->column_attributes[i].get_data_type()});

    ValueDict *validatedRow = new ValueDict();
    for (auto const &item : *row) {
        Identifier colName = item.first;
        Value colVal = item.second;
        auto search = identifierToDataTypeMap.find(colName);
        if (search == identifierToDataTypeMap.end())
            throw DbRelationError("Invalid column name");
        if (colVal.data_type != identifierToDataTypeMap[colName])
            throw DbRelationError("Invalid column type");
        validatedRow->insert({colName, colVal});
    }

    return validatedRow;
}

Handle HeapTable::append(const ValueDict *row) {
    RecordID recordID = 0;
    SlottedPage *page;
    Dbt *data = this->marshal(row);
    BlockIDs *blockIDs = this->file.block_ids();
    auto cleanup = [&]() {
        delete page;
        delete data;
        delete blockIDs;
    };
    for (BlockID &blockID : *blockIDs) {
        page = this->file.get(blockID);
        try {
            recordID = page->add(data);
            this->file.put(page);
            cleanup();
            return Handle(blockID, recordID);
        } catch (DbBlockNoRoomError &e) {
            delete page;
        }
    }

    // no room in any existing blocks - create a new block
    page = this->file.get_new();
    recordID = page->add(data);
    BlockID blockID = page->get_block_id();
    this->file.put(page);
    cleanup();

    return Handle(blockID, recordID);
}

Dbt *HeapTable::marshal(const ValueDict *row) {
    char bytes[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;

    for (auto const &column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        ColumnAttribute::DataType type = ca.get_data_type();
        if (type == ColumnAttribute::INT) {
            auto size = sizeof(value.n);  // n is int32_t we want 4 bytes
            memcpy(bytes + offset, &value.n, size);
            offset += size;
        } else if (type == ColumnAttribute::TEXT) {
            auto size = sizeof(
                u_int16_t);  // assume string length fits in 2 bytes (64k)
            u_int16_t len = value.s.length();
            memcpy(bytes + offset, &len, size);
            offset += size;
            memcpy(bytes + offset, value.s.c_str(),
                   len);  // assume ascii for now
            offset += len;
        } else {
            throw DbRelationError("Only supports marshaling INT and TEXT");
        }
    }

    if (offset > DbBlock::BLOCK_SZ)
        throw DbRelationError("Row too large to marshal");

    char *trimmedBytes = new char[offset];
    memcpy(trimmedBytes, bytes, offset);

    return new Dbt(trimmedBytes, offset);
}

ValueDict *HeapTable::unmarshal(Dbt *data) {
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;

    ValueDict *row = new ValueDict();
    for (auto const &column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        auto type = ca.get_data_type();
        if (type == ColumnAttribute::INT) {
            int32_t n;
            auto size = sizeof(int32_t);
            memcpy(&n, bytes + offset, size);
            row->insert({column_name, Value(n)});
            offset += size;
        } else if (type == ColumnAttribute::TEXT) {
            u_int16_t len;
            auto size = sizeof(u_int16_t);
            memcpy(&len, bytes + offset, size);
            offset += size;
            std::string s(bytes + offset, len);
            row->insert({column_name, Value(s)});
            offset += len;
        } else {
            throw DbRelationError("Only supports unmarshaling INT and TEXT");
        }
    }

    return row;
}

bool test_heap_table() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();
    std::cout << "drop ok" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;

    int32_t n = (*result)["a"].n;
    std::string s = (*result)["b"].s;
    if (n != 12 || s != "Hello!") {
        return false;
    }

    table.drop();
    std::cout << "drop table ok" << std::endl;

    return true;
}
