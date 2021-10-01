package org.noise_planet.noisemodelling.work

import groovy.sql.Sql
import org.h2gis.functions.factory.H2GISDBFactory
import org.h2gis.utilities.SFSUtilities

import java.sql.Connection
import java.sql.DriverManager

class CarquefouWithSncf {

    public static void main(String[] args) {

        def hourTimeStrings = ["0_1", "1_2", "2_3", "3_4", "4_5", "5_6", "6_7", "7_8", "8_9", "9_10", "10_11", "11_12", "12_13", "13_14", "14_15", "15_16", "16_17", "17_18", "18_19", "19_20", "20_21", "21_22", "22_23", "23_24"]
        def quarterHourTimeStrings = ["0h00_0h15", "0h15_0h30", "0h30_0h45", "0h45_1h00", "1h00_1h15", "1h15_1h30", "1h30_1h45", "1h45_2h00", "2h00_2h15", "2h15_2h30", "2h30_2h45", "2h45_3h00", "3h00_3h15", "3h15_3h30", "3h30_3h45", "3h45_4h00", "4h00_4h15", "4h15_4h30", "4h30_4h45", "4h45_5h00", "5h00_5h15", "5h15_5h30", "5h30_5h45", "5h45_6h00", "6h00_6h15", "6h15_6h30", "6h30_6h45", "6h45_7h00", "7h00_7h15", "7h15_7h30", "7h30_7h45", "7h45_8h00", "8h00_8h15", "8h15_8h30", "8h30_8h45", "8h45_9h00", "9h00_9h15", "9h15_9h30", "9h30_9h45", "9h45_10h00", "10h00_10h15", "10h15_10h30", "10h30_10h45", "10h45_11h00", "11h00_11h15", "11h15_11h30", "11h30_11h45", "11h45_12h00", "12h00_12h15", "12h15_12h30", "12h30_12h45", "12h45_13h00", "13h00_13h15", "13h15_13h30", "13h30_13h45", "13h45_14h00", "14h00_14h15", "14h15_14h30", "14h30_14h45", "14h45_15h00", "15h00_15h15", "15h15_15h30", "15h30_15h45", "15h45_16h00", "16h00_16h15", "16h15_16h30", "16h30_16h45", "16h45_17h00", "17h00_17h15", "17h15_17h30", "17h30_17h45", "17h45_18h00", "18h00_18h15", "18h15_18h30", "18h30_18h45", "18h45_19h00", "19h00_19h15", "19h15_19h30", "19h30_19h45", "19h45_20h00", "20h00_20h15", "20h15_20h30", "20h30_20h45", "20h45_21h00", "21h00_21h15", "21h15_21h30", "21h30_21h45", "21h45_22h00", "22h00_22h15", "22h15_22h30", "22h30_22h45", "22h45_23h00", "23h00_23h15", "23h15_23h30", "23h30_23h45", "23h45_24h00"];

        String dbName = "file:///home/valoo/Projects/IFSTTAR/Scenarios/output_edgt_100p/simulation_output/carquefou_100p/noise_modelling_with_scnf.db"
        String timeSlice = "quarter";
        String osmFile = "/home/valoo/Projects/IFSTTAR/OsmMaps/nantes.pbf";
        String matsimFolder = "/home/valoo/Projects/IFSTTAR/Scenarios/output_edgt_100p/simulation_output/carquefou_100p/simulation_output"
        String resultsFolder = "/home/valoo/Projects/IFSTTAR/Scenarios/output_edgt_100p/simulation_output/carquefou_100p/noise_output_with_sncf"
        String ignoreAgents = ""

        boolean createDB = false;
        boolean postgis = false;

        boolean doCleanDB = false;
        boolean doImportBuildings = false;
        boolean doImportMatsimTraffic = false;
        boolean doCreateReceiversFromMatsim = false;
        boolean doCalculateNoisePropagation = false;
        boolean doCalculateNoiseMap = false;
        boolean doExportResults = false;
        boolean doCalcuateExposure = true;

        Connection connection;

        if (!postgis) {
            if (createDB) {
                connection = SFSUtilities.wrapConnection(H2GISDBFactory.createSpatialDataBase(dbName, true));
            }
            else {
                connection = SFSUtilities.wrapConnection(H2GISDBFactory.openSpatialDataBase(dbName));
            }
        }

        if (postgis) {
            String url = "jdbc:postgresql://localhost/postgis_db";
            Properties props = new Properties();
            props.setProperty("user", "postgis_user");
            props.setProperty("password", "postgis");
            //props.setProperty("ssl","true");
            connection = DriverManager.getConnection(url, props);
        }

        if (doCleanDB) {
            CleanDB.cleanDB(connection);
        }
        if (doImportBuildings) {
            ImportBuildings.importBuildings(connection, [
                    "pathFile"        : osmFile,
                    "targetSRID"      : 2154,
                    "ignoreBuilding"  : false,
                    "ignoreGround"    : true,
                    "ignoreRoads"     : true
            ]);
        }
        if (doImportMatsimTraffic) {
            ImportMatsimTraffic.importMatsimTraffic(connection, [
                    "folder" : matsimFolder,
                    "outTableName" : "MATSIM_ROADS",
                    "link2GeometryFile" : matsimFolder + "/network.csv", // absolute path
                    "timeSlice": timeSlice, // DEN, hour, quarter
                    "skipUnused": "true",
                    "exportTraffic": "true",
                    "SRID" : 2154,
                    "ignoreAgents": ignoreAgents
            ]);
        }
        if (doCreateReceiversFromMatsim) {
            CreateReceiversOnBuildings.createReceiversOnBuildings(connection);
            ImportActivitesFromMatsim.importActivitesFromMatsim(connection, [
                    "facilitiesPath" : matsimFolder + "/output_facilities.xml.gz",
                    "SRID" : 2154,
                    "outTableName" : "ACTIVITIES"
            ]);
            // ChoseReceiversFromActivities.choseReceiversFromActivities(connection);
             AssignRandomReceiversToActivities.activitiesRandomReceiver(connection);
        }

        if (doCalculateNoisePropagation) {
            Create0dBSourceFromRoads.create0dBSourceFromRoads(connection);
            CalculateNoiseMapFromSource.calculateNoiseMap(connection, [
                    "tableBuilding": "BUILDINGS",
                    "tableReceivers" : "ACTIVITIES_RECEIVERS",
                    "tableSources" : "SOURCES_0DB",
                    "confMaxSrcDist": 350,
                    "confMaxReflDist": 25,
                    "confReflOrder": 1,
                    "confSkipLevening": true,
                    "confSkipLnight": true,
                    "confSkipLden": true,
                    "confThreadNumber": 14,
                    "confExportSourceId": true,
                    "confDiffVertical": false,
                    "confDiffHorizontal": false
            ]);
        }

        List<String> timeStrings = (timeSlice == "hour") ? hourTimeStrings : quarterHourTimeStrings;

        if (doCalculateNoiseMap) {
            Sql sql = new Sql(connection)

            String outTableName = "RESULT_GEOM"
            String attenuationTable = "LDAY_GEOM"
            String matsimRoads = "MATSIM_ROADS"
            String matsimRoadsStats = "MATSIM_ROADS_STATS"

            sql.execute(String.format("DROP TABLE IF EXISTS %s", outTableName))
            String query = "CREATE TABLE " + outTableName + '''( 
                    PK integer PRIMARY KEY AUTO_INCREMENT,
                    IDRECEIVER integer,
                    THE_GEOM geometry,
                    HZ63 double precision,
                    HZ125 double precision,
                    HZ250 double precision,
                    HZ500 double precision,
                    HZ1000 double precision,
                    HZ2000 double precision,
                    HZ4000 double precision,
                    HZ8000 double precision,
                    TIMESTRING varchar
                )
            '''
            sql.execute(query)
            long count_rec = 0
            sql.query("SELECT COUNT(DISTINCT IDRECEIVER) as count_rec FROM " + attenuationTable, { row ->
                while (row.next()) {
                    count_rec = row.getLong("count_rec")
                }
            })

            sql.eachRow("SELECT DISTINCT IDRECEIVER FROM " + attenuationTable, { row ->
                long i = 1
                long do_print = 1
                long start = System.currentTimeMillis();
                println String.format("-------- Start Processing at : %tT", start)
                while (row.next()) {
                    long rec_id = row.getLong("IDRECEIVER")
                    if (i >= do_print) {
                        long elapsedTime = System.currentTimeMillis() - start
                        double elapsedSec = elapsedTime / 1000.0
                        int sec = (int) elapsedSec % 60
                        int min = (int) (elapsedSec / 60) % 60
                        int hour = (int) (elapsedSec / 3600)
                        println String.format("Processing receiver #%d (max: %d) - elapsed time : %02d:%02d:%02d.%03d", i, count_rec, hour, min, sec, elapsedTime % 1000)
                        do_print *= 2
                    }

                    String query_rec = "INSERT INTO " + outTableName + '''
                        SELECT NULL, lg.IDRECEIVER,  lg.THE_GEOM,
                            10 * LOG10( SUM(POWER(10,(mrs.LW63 + lg.HZ63) / 10))) AS HZ63,
                            10 * LOG10( SUM(POWER(10,(mrs.LW125 + lg.HZ125) / 10))) AS HZ125,
                            10 * LOG10( SUM(POWER(10,(mrs.LW250 + lg.HZ250) / 10))) AS HZ250,
                            10 * LOG10( SUM(POWER(10,(mrs.LW500 + lg.HZ500) / 10))) AS HZ500,
                            10 * LOG10( SUM(POWER(10,(mrs.LW1000 + lg.HZ1000) / 10))) AS HZ1000,
                            10 * LOG10( SUM(POWER(10,(mrs.LW2000 + lg.HZ2000) / 10))) AS HZ2000,
                            10 * LOG10( SUM(POWER(10,(mrs.LW4000 + lg.HZ4000) / 10))) AS HZ4000,
                            10 * LOG10( SUM(POWER(10,(mrs.LW8000 + lg.HZ8000) / 10))) AS HZ8000,
                            mrs.TIMESTRING AS TIMESTRING
                        FROM ''' + attenuationTable + '''  lg 
                        INNER JOIN ''' + matsimRoads + ''' mr ON lg.IDSOURCE = mr.PK
                        INNER JOIN ''' + matsimRoadsStats + ''' mrs ON mr.LINK_ID = mrs.LINK_ID
                        WHERE IDRECEIVER = ''' + rec_id + '''
                        GROUP BY lg.IDRECEIVER, lg.THE_GEOM, mrs.TIMESTRING
                    '''
                    sql.execute(query_rec)
                    i++
                }
            })
        }

        if (doExportResults) {
            ExportTable.exportTable(connection, [
                    "tableToExport": "RESULT_GEOM",
                    "exportPath"   : resultsFolder + "/RESULTS.shp"
            ])
            /*
            for (timeString in timeStrings) {
                org.noise_planet.noisemodelling.work.ExportTable.exportTable(connection, [
                        "tableToExport": "RESULT_GEOM_" + timeString,
                        "exportPath"   : resultsFolder + "/RES_" + timeString + ".geojson"
                ])
                org.noise_planet.noisemodelling.work.ExportTable.exportTable(connection, [
                        "tableToExport": "ALT_RESULT_GEOM_" + timeString,
                        "exportPath"   : resultsFolder + "/ALT_RES_" + timeString + ".geojson"
                ])
                org.noise_planet.noisemodelling.work.ExportTable.exportTable(connection, [
                        "tableToExport": "DIFF_RESULT_GEOM_" + timeString,
                        "exportPath"   : resultsFolder + "/DIFF_RES_" + timeString + ".geojson"
                ])
            }
            */
        }
        if (doCalcuateExposure) {
            CalculateMatsimAgentExposure.calculateMatsimAgentExposure(connection, [
                    "plansFile" : matsimFolder + "/output_plans.xml.gz",
                    "receiversTable": "ACTIVITIES_RECEIVERS",
                    "outTableName" : "EXPOSURES",
                    "dataTable": "RESULT_GEOM",
                    "timeSlice": timeSlice // DEN, hour, quarter
            ])
        }

        connection.close()
    }
}

