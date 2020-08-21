-- Main data tables - Run following DDL statements for each year in your database
CREATE TABLE `tblDump1995` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `Volume` INTEGER, 
    PRIMARY KEY(`Symbol`,`Date`) )

CREATE TABLE `tblModDump1995` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `Volume` INTEGER, 
    `AdjustedOpen` REAL, 
    `AdjustedHigh` REAL, 
    `AdjustedLow` REAL, 
    `AdjustedClose` REAL, 
    PRIMARY KEY(`Symbol`,`Date`) )

CREATE UNIQUE INDEX `idxDump1995` ON `tblDump1995` ( `Symbol` ASC, `Date` ASC )
CREATE UNIQUE INDEX `idxModDump1995` ON `tblModDump1995` ( `Symbol` ASC, `Date` ASC )

-- Other data tables

CREATE TABLE `tblDumpReplace` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `Volume` INTEGER, 
    PRIMARY KEY(`Symbol`,`Date`) )

CREATE TABLE `tblDuplicates` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `Volume` INTEGER )

CREATE TABLE `tblHistIndex` ( 
    `IndexName` TEXT, 
    `Symbol` TEXT, 
    `Date` TEXT )

CREATE TABLE `tblMultipliers` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Multiplier` REAL, 
    `ResultantMultiplier` REAL, 
    PRIMARY KEY(`Symbol`,`Date`) )

CREATE TABLE `tblSkipped` ( 
    `Symbol` TEXT, 
    `Date` TEXT, 
    `Open` REAL, 
    `High` REAL, 
    `Low` REAL, 
    `Close` REAL, 
    `Volume` INTEGER )

CREATE TABLE "tblSymbolRange" (
	"Symbol"	TEXT,
	"TableSource"	TEXT,
	"StartDate"	TEXT,
	"EndDate"	TEXT
)

CREATE UNIQUE INDEX `idxDumpReplace` ON `tblDumpReplace` ( `Symbol` ASC, `Date` ASC )
CREATE INDEX `idxDuplicates` ON `tblDuplicates` ( `Symbol` ASC, `Date` ASC )
CREATE INDEX `idxHistIndex` ON `tblHistIndex` ( `IndexName` ASC, `Date` ASC )
CREATE UNIQUE INDEX `idxMultipliers` ON `tblMultipliers` ( `Symbol` ASC, `Date` ASC )
CREATE INDEX `idxSkipped` ON `tblSkipped` ( `Symbol` ASC, `Date` ASC )
CREATE INDEX "idxSymbolRange" ON "tblSymbolRange" (
	"TableSource"	ASC,
	"Symbol"	ASC,
	"StartDate"	ASC,
	"EndDate"	ASC
)