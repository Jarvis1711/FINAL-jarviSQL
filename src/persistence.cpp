#include "persistence.hpp"
#include "sql_parser.hpp"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace jarvisql {

Persistence::Persistence(const std::string& data_dir) : data_dir_(data_dir) {
  fs::create_directories(data_dir_);
}

std::string Persistence::get_database_dir(const std::string& database) const {
  return data_dir_ + "/" + database;
}

std::string Persistence::get_table_file(const std::string& database, const std::string& table) const {
  return get_database_dir(database) + "/" + table + ".csv";
}

std::string Persistence::get_schema_file(const std::string& database, const std::string& table) const {
  return get_database_dir(database) + "/" + table + ".schema";
}

bool Persistence::save_table(const std::string& database, const std::string& table,
                             const std::vector<ColumnDef>& schema,
                             const std::vector<std::pair<std::vector<std::string>, int64_t>>& rows,
                             std::string& error) {
  try {
    fs::create_directories(get_database_dir(database));

    // Find primary key index
    int pk_index = -1;
    for (size_t i = 0; i < schema.size(); ++i) {
      if (schema[i].primary_key) {
        pk_index = i;
        break;
      }
    }

    // Save schema file
    std::ofstream schema_file(get_schema_file(database, table));
    if (!schema_file) {
      error = "Cannot write schema file";
      return false;
    }
    schema_file << pk_index << "\n";
    for (const auto& col : schema) {
      schema_file << col.name << "|" << static_cast<int>(col.type) << "\n";
    }
    schema_file.close();

    // Save data file
    std::ofstream data_file(get_table_file(database, table));
    if (!data_file) {
      error = "Cannot write data file";
      return false;
    }
    for (const auto& [values, expires_at_ms] : rows) {
      for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) data_file << "|";
        // Escape pipes and newlines
        std::string escaped = values[i];
        size_t pos = 0;
        while ((pos = escaped.find('|', pos)) != std::string::npos) {
          escaped.replace(pos, 1, "\\|");
          pos += 2;
        }
        pos = 0;
        while ((pos = escaped.find('\n', pos)) != std::string::npos) {
          escaped.replace(pos, 1, "\\n");
          pos += 2;
        }
        data_file << escaped;
      }
      data_file << "|" << expires_at_ms << "\n";
    }
    data_file.close();
    return true;
  } catch (const std::exception& e) {
    error = std::string("Persistence error: ") + e.what();
    return false;
  }
}

bool Persistence::load_table(const std::string& database, const std::string& table,
                             std::vector<ColumnDef>& schema,
                             std::vector<std::pair<std::vector<std::string>, int64_t>>& rows,
                             int& primary_key_index,
                             std::string& error) {
  try {
    std::string schema_path = get_schema_file(database, table);
    if (!fs::exists(schema_path)) {
      return true;  // Table doesn't exist on disk, start fresh
    }

    // Load schema
    std::ifstream schema_file(schema_path);
    if (!schema_file) {
      error = "Cannot read schema file";
      return false;
    }
    schema_file >> primary_key_index;
    std::string line;
    while (std::getline(schema_file, line)) {
      if (line.empty()) continue;
      size_t pipe_pos = line.find('|');
      if (pipe_pos == std::string::npos) continue;
      std::string col_name = line.substr(0, pipe_pos);
      int type_int = std::stoi(line.substr(pipe_pos + 1));
      ColumnDef col;
      col.name = col_name;
      col.type = static_cast<ColumnType>(type_int);
      col.primary_key = (static_cast<int>(schema.size()) == primary_key_index);
      schema.push_back(col);
    }
    schema_file.close();

    // Load data
    std::string data_path = get_table_file(database, table);
    if (!fs::exists(data_path)) {
      return true;  // No data yet
    }

    std::ifstream data_file(data_path);
    if (!data_file) {
      error = "Cannot read data file";
      return false;
    }

    while (std::getline(data_file, line)) {
      if (line.empty()) continue;
      
      std::vector<std::string> values;
      int64_t expires_at_ms = -1;
      
      std::string current_val;
      bool escaped = false;
      for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        if (escaped) {
          if (c == '|') current_val += '|';
          else if (c == 'n') current_val += '\n';
          else current_val += c;
          escaped = false;
        } else if (c == '\\') {
          escaped = true;
        } else if (c == '|') {
          values.push_back(current_val);
          current_val.clear();
        } else {
          current_val += c;
        }
      }
      if (!current_val.empty()) {
        // Last field is expiry time
        expires_at_ms = std::stoll(current_val);
      }
      if (!values.empty()) {
        rows.push_back({values, expires_at_ms});
      }
    }
    data_file.close();
    return true;
  } catch (const std::exception& e) {
    error = std::string("Load error: ") + e.what();
    return false;
  }
}

bool Persistence::list_databases(std::vector<std::string>& databases, std::string& error) {
  try {
    if (!fs::exists(data_dir_)) {
      return true;
    }
    for (const auto& entry : fs::directory_iterator(data_dir_)) {
      if (entry.is_directory()) {
        databases.push_back(entry.path().filename().string());
      }
    }
    return true;
  } catch (const std::exception& e) {
    error = std::string("Error listing databases: ") + e.what();
    return false;
  }
}

bool Persistence::list_tables(const std::string& database, std::vector<std::string>& tables, std::string& error) {
  try {
    std::string db_dir = get_database_dir(database);
    if (!fs::exists(db_dir)) {
      return true;
    }
    for (const auto& entry : fs::directory_iterator(db_dir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        if (filename.size() > 6 && filename.substr(filename.size() - 6) == ".schema") {
          std::string table_name = filename.substr(0, filename.size() - 7);
          tables.push_back(table_name);
        }
      }
    }
    return true;
  } catch (const std::exception& e) {
    error = std::string("Error listing tables: ") + e.what();
    return false;
  }
}

bool Persistence::table_exists(const std::string& database, const std::string& table) {
  return fs::exists(get_schema_file(database, table));
}

bool Persistence::database_exists(const std::string& database) {
  return fs::exists(get_database_dir(database)) && fs::is_directory(get_database_dir(database));
}

}  // namespace jarvisql
