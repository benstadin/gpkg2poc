//
//  main.cpp
//  GPKGTest
//
//  Created by Stadin Benjamin on 03.04.17.
//  Copyright © 2017 Stadin Benjamin. All rights reserved.
//

#include <iostream>
#include <assert.h>
#include <unordered_map>
#include "sqlite3.h"
//#include "sqlite3ext.h"

sqlite3 *_db = NULL;

typedef struct ContentTableInfo {
    int srs_id;
    int gpkg_version;
} ContentTableInfo;

std::unordered_map<std::string, ContentTableInfo*> _contentTableCache;

ContentTableInfo *getContentTableInfo(const char* table)
{
    ContentTableInfo *info = NULL;
    sqlite3_stmt *stmt;
    
    char sql[256];
    sprintf(sql, "SELECT srs_id, gpkg_version FROM gpkg_contents WHERE table_name = '%s'", table);
    
    int rc = sqlite3_prepare_v2(_db, sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create statement to fetch info from content table");
    }
    
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        info = new ContentTableInfo;
        info->srs_id = sqlite3_column_int(stmt, 0);
        info->gpkg_version = sqlite3_column_int(stmt, 1);
    }
    
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    
    return info;
}

static void GPKGInsertTrigger(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    ContentTableInfo *contentTableInfo = nullptr;          /* cached content table info object */
    bool storeInfo = false;           /* invoke sqlite3_set_auxdata() to store the info for the table */
    
    assert(argc == 3);
    
    const char *table = (const char*)sqlite3_value_text(argv[0]);
    const char *action = (const char*)sqlite3_value_text(argv[1]);
    const char *envelopeBinary = (const char*)sqlite3_value_text(argv[2]);
    
    if( !table || !action ){
        sqlite3_result_error(context, "Wrong params provided", -1);
        return;
    }
    
    // Try to get the associated info (srs_id and gpkg_version) from the auxilary data, which use as a cache.
    // If not already assigned, fetch info from actual table.
    auto iter = _contentTableCache.find(table);
    if( iter == _contentTableCache.end() ){
        // Fetch from gpkg_contents
        fprintf(stderr, "Content info not yet cached, fetching info from gpkg_contents\n");
        contentTableInfo = getContentTableInfo(table);
        if( contentTableInfo==0 ){
            sqlite3_result_error(context, "Content table info for table not found", -1);
            return;
        }
        storeInfo = true;
    }
    else {
        fprintf(stderr, "Reusing cached content info\n");
        contentTableInfo = iter->second;
    }
    
    if( storeInfo ){
        _contentTableCache[table] = contentTableInfo;
    }
    
    sqlite3_result_int(context, SQLITE_OK);
    
    fprintf(stderr, "Feature table changed: table = %s, action = %s, envelope = %s\n", table, action, envelopeBinary);
    fprintf(stderr, "srs_id = %d, gpkg_version = %d\n", contentTableInfo->srs_id, contentTableInfo->gpkg_version);
    fprintf(stderr, "envelope = '%s'\n", envelopeBinary);
    
    // todo: just use envelopeBinary here together with srs_id to actually update the rtree
}

int execSql(sqlite3 *db, char *sql)
{
    char *zErrMsg = NULL;
    int rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    return rc;
}

int main(int argc, const char * argv[]) {
    char contentTableSql[] = "CREATE TABLE gpkg_contents ( "\
                                                               "table_name TEXT NOT NULL PRIMARY KEY,"\
                                                               "data_type TEXT NOT NULL,"\
                                                               "identifier TEXT UNIQUE,"\
                                                               "description TEXT DEFAULT '',"\
                                                               "last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),"\
                                                               "min_x DOUBLE,"\
                                                               "min_y DOUBLE,"\
                                                               "max_x DOUBLE,"\
                                                               "max_y DOUBLE,"\
                                                               "srs_id INTEGER,"\
                                                               "gpkg_version"\
                                                               ");";
    
    char contentTableInsertSql[] = "INSERT INTO gpkg_contents "\
        "(table_name,data_type,identifier,description,last_change,min_x,min_y,max_x,max_y,srs_id,gpkg_version) "\
        "VALUES ('sample_features','WKB','','sample feature table',date('now'),0,0,1,1,4711,2)";
    
    char sampleFeatureTableSql[] = "CREATE TABLE sample_features (feature_id INTEGER PRIMARY KEY NOT NULL,"\
        "envelope BLOB,geom BLOB,feature_name TEXT);";
    
    char sampleFeatureInsertSql[] = "INSERT INTO sample_features (feature_id,"\
    "envelope,geom,feature_name) VALUES (?, ?, 'geomBinary', 'someName');";
    
    char sampleRtreeTableSql[] = "CREATE VIRTUAL TABLE demo_index USING rtree(id,minX, maxX,minY, maxY)";
    
    char insertTriggerSql[] = "CREATE TRIGGER gpkg_rtree_trigger AFTER INSERT ON sample_features BEGIN "\
        "SELECT GPKGInsertTrigger('sample_features', 'INSERT', NEW.envelope); END;";
    
    
    int rc;
    
    rc = sqlite3_open("tmp.sqlite3", &_db);
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(_db));
        sqlite3_close(_db);
        return 1;
    }
    
    // Register the extension function that will take care of updating the rtree index
    rc = sqlite3_create_function_v2(_db, (const char *)"GPKGInsertTrigger", 3, SQLITE_UTF8, 0, GPKGInsertTrigger, 0, 0, 0);
    
    assert(rc == SQLITE_OK);
    
    if (execSql(_db, contentTableSql))
    {
        sqlite3_close(_db);
        return 1;
    }
    
    if (execSql(_db, contentTableInsertSql))
    {
        sqlite3_close(_db);
        return 1;
    }
    
    if (execSql(_db, sampleFeatureTableSql))
    {
        sqlite3_close(_db);
        return 1;
    }
    
    if (execSql(_db, sampleRtreeTableSql))
    {
        sqlite3_close(_db);
        return 1;
    }
    
    if (execSql(_db, insertTriggerSql))
    {
        sqlite3_close(_db);
        return 1;
    }
    
    // insert something and watch the trigger method fire
    sqlite3_stmt *insertStmt;
    rc = sqlite3_prepare_v2(_db, sampleFeatureInsertSql, -1, &insertStmt, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare insert statement for feature table");
    }
    
    sqlite3_bind_int(insertStmt, 1, 1);
    sqlite3_bind_text(insertStmt, 2, "envelopeBinary1", -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(insertStmt);
    if (rc == SQLITE_OK) {
        fprintf(stderr, "Failed to insert first row into feature table");
        sqlite3_finalize(insertStmt);
    }
    sqlite3_reset(insertStmt);
    
    // insert a second time to validate our gpkg_contents cache works
    sqlite3_bind_int(insertStmt, 1, 2);
    sqlite3_bind_text(insertStmt, 2, "envelopeBinary2", -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(insertStmt);
    if (rc == SQLITE_OK) {
        fprintf(stderr, "Failed to insert first row into feature table");
        sqlite3_finalize(insertStmt);
    }
    
    sqlite3_finalize(insertStmt);
    
    sqlite3_close(_db);
    
    for (auto iter : _contentTableCache) {
        delete iter.second;
    }
    
    return 0;
}



