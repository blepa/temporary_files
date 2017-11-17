USE [DCMS_Staging_Area]
GO

/****** Object:  StoredProcedure [stgexp].[hdb_ADMIN_export_tables]    Script Date: 11/17/2017 3:05:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [stgexp].[hdb_ADMIN_export_tables]
	@SourceInstance VARCHAR(100),
	@SourceDBName VARCHAR(100),
	@SourcePath NVARCHAR(1000),
	@TargetInstance VARCHAR(100),
	@TargetDBName VARCHAR(100),
	@TargetPath NVARCHAR(1000),
	@SubsetName NVARCHAR(100)=''
--WITH EXECUTE AS OWNER
AS
--deklaracje zmiennych
DECLARE @SourceSchema VARCHAR(100);
DECLARE @TargetSchema VARCHAR(100);
DECLARE @VarIDExport BIGINT
DECLARE @VarSourceTable VARCHAR(500);
DECLARE @VarTargetTable VARCHAR(500);
DECLARE @VarWherePredicate NVARCHAR(4000);
DECLARE @VarColsList NVARCHAR(MAX);
DECLARE @SqlSelect NVARCHAR(MAX);
DECLARE @SqlExport NVARCHAR(MAX);
DECLARE @VarExpFileName VARCHAR(500);
DECLARE @VarErrFileName VARCHAR(500);
DECLARE @HostName VARCHAR(100) = COALESCE(@SourceInstance, @@SERVERNAME);
DECLARE @TargetHostName VARCHAR(100) = COALESCE(@TargetInstance, @@SERVERNAME);
DECLARE @VarBCPResultsTable VARCHAR(500);
DECLARE @VarBCPResultsFinalTable VARCHAR(500);
DECLARE @VarBCPResultsCnt BIGINT;
DECLARE @VarSqlStatement NVARCHAR(MAX);
DECLARE @SourceDBNameDefault VARCHAR(100) = @SourceDBName;
DECLARE @TargetDBNameDefault VARCHAR(100) = @TargetDBName;
DECLARE @VarCurrentDate CHAR(14) = REPLACE(REPLACE(REPLACE(CONVERT(CHAR(20), GETDATE(), 120),'-',''),':',''),' ','');
DECLARE @VarResultsFileName VARCHAR(500);
DECLARE @VarImpFileName VARCHAR(500);
DECLARE @VarImportSQLTempTable VARCHAR(500);

BEGIN
	BEGIN TRY
	SELECT @SourcePath = [stgmd].[hdb_get_environment_variable_value]('FILE_DISK_LETTER') + 'DWHUPLOAD\00_EXPORT_IMPORT'
	print @SourcePath
		SET @SubsetName = COALESCE(@SubsetName,'');
		SET @SourcePath = LTRIM(RTRIM(@SourcePath));
		SET @TargetPath = LTRIM(RTRIM(@TargetPath));
		IF RIGHT(@SourcePath,1) = '\'
			BEGIN
				SET @SourcePath = SUBSTRING(@SourcePath,1,LEN(@SourcePath)-1);
			END
		IF RIGHT(@TargetPath,1) = '\'
			BEGIN
				SET @TargetPath = SUBSTRING(@TargetPath,1,LEN(@TargetPath)-1);
			END

		--deklaracja kursora przechodzącego po tabeli [stgexp].[export_tables]
		DECLARE CUR_LIST CURSOR LOCAL STATIC
		FOR
		SELECT
			[id_export]
			,[source_schema]
		  ,[source_table]
		  ,[target_schema]
		  ,[target_table]
		  ,[where_predicate]
		FROM
			[stgexp].[export_tables]
		WHERE
			1=1
			AND [active_flag]='Y'
			AND [subset_name]= CASE WHEN @SubsetName='' THEN [subset_name] ELSE @SubsetName END
		ORDER BY
			[id_export]



		OPEN CUR_LIST
		FETCH NEXT FROM CUR_LIST
		INTO
		@VarIDExport, @SourceSchema, @VarSourceTable, @TargetSchema, @VarTargetTable, @VarWherePredicate

		WHILE @@FETCH_STATUS=0
			BEGIN

				--sprawdzenie czy tabela istnieje
				IF (SELECT
					COUNT(*)
				FROM
					INFORMATION_SCHEMA.TABLES
				WHERE 
					1=1
					AND TABLE_CATALOG=@SourceDBName
					AND TABLE_SCHEMA=@SourceSchema
					AND TABLE_NAME=@VarSourceTable)=0
					BEGIN
						print 'Tabela ' + @SourceDBName + '.' + @SourceSchema + '.' + @VarSourceTable + ' nie istnieje.'
						GOTO Cont
					END


				--Nazwy pliku i tabele
				SET @VarImportSQLTempTable = '##IMP_' +@SourceDBName + '_' + @SourceSchema + '_' + @VarSourceTable + '_' + CAST(@VarIDExport AS VARCHAR(50));
				SELECT @VarExpFileName = 'EXP__' + @SourceDBName + '__' + @SourceSchema + '__' + @VarSourceTable + '__' + @VarCurrentDate + '.TXT';
				SET @VarImpFileName = 'EXP__' + @SourceDBName + '__' + @SourceSchema + '__' + @VarSourceTable + '__' + @VarCurrentDate + '.SQL';
				SET @VarResultsFileName = 'EXP__' + @SourceDBName + '__' + @SourceSchema + '__' + @VarSourceTable + '__' + @VarCurrentDate + '.RES';
				SET @VarErrFileName = 'EXP__' + @SourceDBName + '__' + @SourceSchema + '__' + @VarSourceTable + '__' + @VarCurrentDate + '.ERR'
				--Tymczasowa tabela przechowująca wynik BCP
				SELECT @VarBCPResultsTable = '##TMP_' +@SourceDBName + '_' + @SourceSchema + '_' + @VarSourceTable + '_' + CAST(@VarIDExport AS VARCHAR(50)) + '_TMP';
				SELECT @VarBCPResultsFinalTable = '##TMP_' +@SourceDBName + '_' + @SourceSchema + '_' + @VarSourceTable + '_' + CAST(@VarIDExport AS VARCHAR(50));


				--stworzenie tabeli tymczasowej z wynikami
				SET @SqlSelect = 
				'
				IF OBJECT_ID(''TEMPDB..' + @VarBCPResultsFinalTable + ''',N''U'') IS NOT NULL DROP TABLE ' + @VarBCPResultsFinalTable + ';
				CREATE TABLE ' + @VarBCPResultsFinalTable + '(
					[id_export] [bigint] NOT NULL,
					[source_schema] [varchar](100) NULL,
					[source_table] [varchar](500) NULL,
					[export_timestamp] [char](14) NULL,
					[exported_records_cnt] [bigint] NULL,
					[imported_records_cnt] [bigint] NULL,
					[subset_name] [nvarchar](100) NULL
				)';
				EXEC (@SqlSelect)

				
				--utworzenie tymczasowej tabeli z sqlami do importu
				SET @SqlSelect=
					'
						IF OBJECT_ID(''TEMPDB..' + @VarImportSQLTempTable + ''',N''U'') IS NOT NULL DROP TABLE ' + @VarImportSQLTempTable + ';

						CREATE TABLE ' + @VarImportSQLTempTable + '(RESULT_TEXT NVARCHAR(4000));
					';
				EXEC (@SqlSelect)


				--pobranie kolumn do selecta
				SELECT @VarColsList = (
					SELECT
						COALESCE(sec.target_value,'[' + isc.COLUMN_NAME + ']')  + CASE WHEN sec.source_column is not null then ' AS [' + sec.source_column + ']' ELSE '' END + ','
					FROM
						INFORMATION_SCHEMA.COLUMNS isc
						LEFT JOIN [stgexp].[export_columns] sec
							ON isc.COLUMN_NAME=sec.source_column
							AND sec.id_export=@VarIDExport
							AND sec.active_flag='Y'
					WHERE
						1=1
						AND isc.TABLE_CATALOG=@SourceDBName
						AND isc.TABLE_SCHEMA=@SourceSchema
						AND isc.TABLE_NAME=@VarSourceTable
					ORDER BY
						isc.ORDINAL_POSITION
				FOR XML PATH, TYPE).value('.[1]', 'nvarchar(max)');
				SELECT @VarColsList = SUBSTRING(@VarColsList,1,LEN(@VarColsList)-1)

				--klauzula WHERE
				IF (LEN(COALESCE(@VarWherePredicate,''))>2)
					BEGIN
						SELECT @VarWherePredicate = 'WHERE ' + @VarWherePredicate
					END
				ELSE
					BEGIN
						SELECT @VarWherePredicate=''
					END


				--SELECT
				SELECT @SqlSelect='"SELECT ' + @VarColsList + ' FROM ' + @SourceDBName + '.' + @SourceSchema + '.' + @VarSourceTable + ' ' + @VarWherePredicate + '"'
				print @SqlSelect



				--kod eksportujący na podstawie selecta
				SET @SqlExport=
				'
					IF OBJECT_ID(''TEMPDB..' + @VarBCPResultsTable + ''',N''U'') IS NOT NULL DROP TABLE ' + @VarBCPResultsTable + ';

					CREATE TABLE ' + @VarBCPResultsTable + '(RESULT_TEXT NVARCHAR(1000));

					INSERT INTO ' + @VarBCPResultsTable + ' 
					EXEC xp_cmdshell ''BCP '+ REPLACE(@SqlSelect,'''','''''') +' QUERYOUT '+@SourcePath+'\'+@VarExpFileName+' -c -t\t -r\n -C ACP -S '+@HostName+' -T '';
				';
				print @SqlExport
				EXEC (@SqlExport)


				--wyciągnięcie ilości rekordów wyeksportowanych
				SET @SqlSelect =
				'
					SELECT 
						@VarBCPResultsCnt = CAST(SUBSTRING(RESULT_TEXT,1,PATINDEX(''%rows%'',RESULT_TEXT)-1) AS BIGINT)
					FROM 
						 ' + @VarBCPResultsTable + ' 
					WHERE 
						RESULT_TEXT LIKE ''%rows copied%''
				'
				EXEC sp_executesql @SqlSelect, N'@VarBCPResultsCnt BIGINT OUTPUT', @VarBCPResultsCnt = @VarBCPResultsCnt OUTPUT;

				print @VarBCPResultsCnt

				--insert do tabeli z rezultatami
				SET @SqlSelect = 
				'
				INSERT INTO ' + @VarBCPResultsFinalTable + ' VALUES(' + CAST(@VarIDExport AS VARCHAR(100)) + ', ''' + @SourceSchema + ''', ''' + @VarSourceTable + ''', ''' + @VarCurrentDate + ''', ' + CAST(@VarBCPResultsCnt AS VARCHAR(100)) + ', NULL, ''' + @SubsetName +''');
				'
				print @SqlSelect
				EXEC (@SqlSelect)

				--insert poleceń sql do importu do tymczasowej tabeli
				SELECT @VarSqlStatement = 		
					'
					--------------------------------------------------------------------------------------------------------
					--import ' + @VarExpFileName + ' 
					--------------------------------------------------------------------------------------------------------

					

						IF OBJECT_ID(''TEMPDB..' + @VarBCPResultsTable + ''',N''U'') IS NOT NULL DROP TABLE ' + @VarBCPResultsTable + ';

						CREATE TABLE ' + @VarBCPResultsTable + '(RESULT_TEXT NVARCHAR(1000));

						TRUNCATE TABLE ' + @TargetDBName + '.' + @TargetSchema + '.' + @VarTargetTable + ';

						INSERT INTO ' + @VarBCPResultsTable + '
						EXEC xp_cmdshell ''BCP ' + @TargetDBName + '.' + @TargetSchema + '.' + @VarTargetTable + ' IN '+@TargetPath+'\'+@VarExpFileName+' -c -t\t -r\n -C ACP -S '+@TargetHostName+' -T -e ' + +@TargetPath+'\'+@VarErrFileName + ''';
						GO

						DECLARE @VarBCPResultsCnt BIGINT = -99999999999;
						SELECT 
							@VarBCPResultsCnt = CAST(SUBSTRING(RESULT_TEXT,1,PATINDEX(''%rows%'',RESULT_TEXT)-1) AS BIGINT)
						FROM 
							 ' + @VarBCPResultsTable + ' 
						WHERE 
							RESULT_TEXT LIKE ''%rows copied%'';

						IF OBJECT_ID(''TEMPDB..' + @VarBCPResultsFinalTable + ''',N''U'') IS NOT NULL DROP TABLE ' + @VarBCPResultsFinalTable + ';
						CREATE TABLE ' + @VarBCPResultsFinalTable + '(
							[id_export] [bigint] NOT NULL,
							[source_schema] [varchar](100) NULL,
							[source_table] [varchar](500) NULL,
							[export_timestamp] [char](14) NULL,
							[exported_records_cnt] [bigint] NULL,
							[imported_records_cnt] [bigint] NULL,
							[subset_name] [nvarchar](100) NULL
							);

						EXEC xp_cmdshell ''BCP ' + @VarBCPResultsFinalTable + ' IN '+@TargetPath+'\'+@VarResultsFileName+' -c -t\t -r\n -C ACP -S '+@TargetHostName+' -T '', no_output;
					
						UPDATE 	' + @VarBCPResultsFinalTable + '
							SET [imported_records_cnt] = @VarBCPResultsCnt
							WHERE id_export = ' + CAST(@VarIDExport AS VARCHAR(50)) + '; 

						SELECT * FROM ' + @VarBCPResultsFinalTable + ';

						EXEC xp_cmdshell ''BCP ' + @VarBCPResultsFinalTable + ' OUT '+@TargetPath+'\' + @VarResultsFileName + ' -c -t\t -r\n -C ACP -S '+@TargetHostName+' -T '', no_output;
					
					
									
					';
				SELECT @VarSqlStatement = RTRIM(LTRIM(REPLACE(REPLACE(REPLACE(REPLACE(@VarSqlStatement,CHAR(9),' '),' ','{}'),'}{',''),'{}',' ')))
				SET @SqlSelect=
					'
					INSERT INTO ' + @VarImportSQLTempTable + ' VALUES(''' + REPLACE(@VarSqlStatement,'''','''''') + ''')
					';
				EXEC(@SqlSelect)

				--eksport tabeli z poleceniami sql do importu
				SET @SqlExport=
				'
					EXEC xp_cmdshell ''BCP '+ @VarImportSQLTempTable +' OUT '+@SourcePath+'\' + @VarImpFileName + ' -c -t\t -r\n -C ACP -S '+@HostName+' -T '', no_output;
				';

				EXEC(@SqlExport)

				--eksport tabeli z rezultatami
				SET @SqlExport=
				'
					EXEC xp_cmdshell ''BCP ' + @VarBCPResultsFinalTable + ' OUT '+@SourcePath+'\' + @VarResultsFileName + ' -c -t\t -r\n -C ACP -S '+@HostName+' -T '', no_output;
				';

				EXEC(@SqlExport)

				Cont:
				FETCH NEXT FROM CUR_LIST
				INTO
				@VarIDExport, @SourceSchema, @VarSourceTable, @TargetSchema, @VarTargetTable, @VarWherePredicate

			END

		CLOSE CUR_LIST
		DEALLOCATE CUR_LIST

		SET @SqlExport=
		'
			exec xp_cmdshell ''COPY /b "' + @SourcePath + '\*' + @VarCurrentDate + '.SQL" "' + @SourcePath + '\import_files_' + @VarCurrentDate + '.SQL"'', no_output;
		';

		EXEC(@SqlExport)


	END TRY

	BEGIN CATCH

		SELECT ERROR_NUMBER() AS ErrorNumber
		,ERROR_SEVERITY() AS ErrorSeverity
		,ERROR_STATE() AS ErrorState
		,ERROR_PROCEDURE() AS ErrorProcedure
		,ERROR_LINE() AS ErrorLine
		,ERROR_MESSAGE() AS ErrorMessage;

	END CATCH




END






GO


