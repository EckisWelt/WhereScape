CREATE PROCEDURE update_hist_demo
 @p_sequence         INT
,@p_job_name         VARCHAR(256)
,@p_task_name        VARCHAR(256)
,@p_job_id           INT
,@p_task_id          INT
,@p_return_msg       VARCHAR(256) OUTPUT
,@p_status           INT          OUTPUT
AS
  SET XACT_ABORT OFF  -- Turn off auto abort on errors
  SET NOCOUNT ON      -- Turn off row count messages

  --============================================================================
  -- Control variables used in most procedures
  --============================================================================
  DECLARE
   @v_msgtext               VARCHAR(255)  -- Text for audit_trail
  ,@v_sql                   NVARCHAR(MAX) -- Text for SQL statements
  ,@v_step                  INT           -- return code
  ,@v_insert_count          INT           -- no of records inserted
  ,@v_update_count          INT           -- no of records updated
  ,@v_change_count          INT           -- Used for history start/end dates
  ,@v_delete_count          INT           -- no of records deleted
  ,@v_batch_count           INT           -- no of batches run
  ,@v_row_count             INT           -- General row count
  ,@v_return_status         INT           -- Update result status
  ,@v_current_datetime      DATETIME2(7)  -- Used for date insert

  --============================================================================
  -- Batch Processing Variables
  --============================================================================
  SET @v_step = 100

  DECLARE @v_batch_start DATETIME2(7)

  --============================================================================
  -- Cursor for Batch processing
  --============================================================================
  SET @v_step = 200

  DECLARE @last_dss_load_datetime DATETIME2(7) = (SELECT ISNULL(MAX(dss_load_datetime) , '0001-01-01') FROM [TABLEOWNER].[hist_demo])

  DECLARE cursor_batch CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
    SELECT DISTINCT dss_load_datetime batch_start
    FROM [TABLEOWNER].[stage_hist_demo] stage_hist_demo
    WHERE dss_load_datetime >= @last_dss_load_datetime
    ORDER BY dss_load_datetime

  --============================================================================
  -- MAIN
  --============================================================================
  SET @v_step = 300

  SET @v_insert_count = 0
  SET @v_update_count = 0
  SET @v_change_count = 0
  SET @v_batch_count = 0
  SET @v_current_datetime = SYSDATETIME()

  BEGIN TRY

    --==========================================================================
    -- Source
    --==========================================================================
    SELECT @v_msgtext = 'Load data from stage_hist_demo'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    OPEN cursor_batch
    FETCH NEXT FROM cursor_batch INTO @v_batch_start

    WHILE @@FETCH_STATUS = 0
      BEGIN
        SET @v_batch_count += 1
        SELECT @v_msgtext = 'Processing batch : ' + COALESCE(CONVERT(VARCHAR,@v_batch_start),' ') + ''
        EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
        
        --======================================================================
        -- Update expiring records
        --======================================================================
        SET @v_step = 400

        UPDATE [TABLEOWNER].[hist_demo] WITH ( TABLOCKX )
        SET    dss_end_datetime = DATEADD(NS,-100,stage_hist_demo.dss_load_datetime)
              ,dss_current_flag = 'N'
              ,dss_update_datetime = @v_current_datetime
        FROM [TABLEOWNER].[stage_hist_demo] stage_hist_demo
        WHERE  stage_hist_demo.dss_load_datetime = @v_batch_start
               AND hist_demo.dss_current_flag = 'Y'
               AND hist_demo.shop_code = stage_hist_demo.shop_code
               AND hist_demo.sid = stage_hist_demo.sid
               AND hist_demo.request_id = stage_hist_demo.request_id
               AND (
                      hist_demo.dss_change_hash <> stage_hist_demo.dss_change_hash
                   )
        ;

        SELECT @v_row_count = @@ROWCOUNT

        SET @v_change_count = @v_change_count + @v_row_count

        SELECT @v_msgtext = 'Changed : ' + CONVERT(VARCHAR,COALESCE(@v_row_count,0)) + ' rows'
        EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
        
        --======================================================================
        -- Insert new records
        --======================================================================
        SET @v_step = 500

        INSERT INTO [TABLEOWNER].[hist_demo] WITH ( TABLOCKX )
        (
               shop_code
              ,sid
              ,request_id
              ,times
              ,order_id
              ,total_price
              ,dss_load_datetime
              ,dss_sequence_no
              ,dss_record_source
              ,dss_change_hash
              ,dss_start_datetime
              ,dss_end_datetime
              ,dss_current_flag
              ,dss_version
              ,dss_create_datetime
              ,dss_update_datetime
        )
        SELECT 
               hist_demo.shop_code AS shop_code 
              ,hist_demo.sid AS sid 
              ,hist_demo.request_id AS request_id 
              ,hist_demo.times AS times 
              ,hist_demo.order_id AS order_id 
              ,hist_demo.total_price AS total_price 
              ,hist_demo.dss_load_datetime AS dss_load_datetime 
              ,hist_demo.dss_sequence_no AS dss_sequence_no 
              ,hist_demo.dss_record_source AS dss_record_source 
              ,hist_demo.dss_change_hash AS dss_change_hash 
              ,hist_demo.dss_start_datetime AS dss_start_datetime 
              ,hist_demo.dss_end_datetime AS dss_end_datetime 
              ,hist_demo.dss_current_flag AS dss_current_flag 
              ,hist_demo.dss_version AS dss_version 
              ,@v_current_datetime AS dss_create_datetime 
              ,@v_current_datetime AS dss_update_datetime 
        FROM   (
          SELECT hist_demo.*
                ,CASE WHEN current_rows.shop_code IS NULL
                      THEN CAST('0001-01-01' AS DATETIME2(7))
                      ELSE hist_demo.dss_load_datetime
                 END AS dss_start_datetime 
                ,CAST('9999-12-31' AS DATETIME2(7)) AS dss_end_datetime 
                ,'Y' AS dss_current_flag 
                ,CASE WHEN current_rows.shop_code IS NULL
                      THEN 1
                      ELSE current_rows.dss_version + 1
                 END AS dss_version 
          FROM   (
            SELECT hist_demo.*
            FROM   (
              SELECT 
                     stage_hist_demo.shop_code AS shop_code 
                    ,stage_hist_demo.sid AS sid 
                    ,stage_hist_demo.request_id AS request_id 
                    ,stage_hist_demo.times AS times 
                    ,stage_hist_demo.order_id AS order_id 
                    ,stage_hist_demo.total_price AS total_price 
                    ,stage_hist_demo.dss_load_datetime AS dss_load_datetime 
                    ,stage_hist_demo.dss_sequence_no AS dss_sequence_no 
                    ,stage_hist_demo.dss_record_source AS dss_record_source 
                    ,stage_hist_demo.dss_change_hash AS dss_change_hash 
              FROM [TABLEOWNER].[stage_hist_demo] stage_hist_demo
              WHERE  stage_hist_demo.dss_load_datetime = @v_batch_start
            ) AS hist_demo
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM   [TABLEOWNER].[hist_demo] hist_demo__not_exist
                    WHERE  hist_demo__not_exist.shop_code = hist_demo.shop_code
                           AND hist_demo__not_exist.sid = hist_demo.sid
                           AND hist_demo__not_exist.request_id = hist_demo.request_id
                           AND hist_demo__not_exist.dss_change_hash = hist_demo.dss_change_hash
                           AND hist_demo__not_exist.dss_current_flag = 'Y'
            )
          ) AS hist_demo
          LEFT OUTER JOIN (
                  SELECT
                         hist_demo__current.shop_code AS shop_code
                        ,hist_demo__current.sid AS sid
                        ,hist_demo__current.request_id AS request_id
                        ,MAX(hist_demo__current.dss_version) AS dss_version
                  FROM  [TABLEOWNER].[hist_demo] hist_demo__current
                  GROUP BY
                         hist_demo__current.shop_code
                        ,hist_demo__current.sid
                        ,hist_demo__current.request_id
          ) AS current_rows
              ON  current_rows.shop_code = hist_demo.shop_code
              AND current_rows.sid = hist_demo.sid
              AND current_rows.request_id = hist_demo.request_id
        ) AS hist_demo
        ;

        SELECT @v_row_count = @@ROWCOUNT

        SET @v_insert_count = @v_insert_count + @v_row_count

        SELECT @v_msgtext = 'Inserted : ' + CONVERT(VARCHAR,COALESCE(@v_row_count,0)) + ' rows'
        EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
        
        IF @v_batch_count % 100 = 0
        BEGIN
          ALTER INDEX ALL ON [TABLEOWNER].[hist_demo] REORGANIZE PARTITION = ALL;
          UPDATE STATISTICS [TABLEOWNER].[hist_demo];
          SELECT @v_msgtext = 'Indexes Reorganized'
          EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
        END
        FETCH NEXT FROM cursor_batch INTO @v_batch_start
      END
    CLOSE cursor_batch
    DEALLOCATE cursor_batch

    --==========================================================================
    -- Finish Up
    --==========================================================================
    SET @v_step = 600

    EXEC WsWrkTask @p_job_id, @p_task_id, @p_sequence, @v_insert_count, @v_update_count, 0, @v_delete_count, 0, 0, 0 

    SET @p_status = 1
    SET @p_return_msg = 'hist_demo updated. '
      + CASE WHEN @v_delete_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_delete_count,0)) + ' records deleted. ' ELSE '' END
      + CONVERT(VARCHAR,COALESCE(@v_insert_count,0)) + ' records added. '
      + CASE WHEN @v_update_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_update_count,0)) + ' records updated. ' ELSE '' END
      + CASE WHEN @v_change_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_change_count,0)) + ' records changed. ' ELSE '' END

    RETURN 0

  END TRY
  BEGIN CATCH

    SET @p_status = -2
    SET @p_return_msg = SUBSTRING('update_hist_demo updated FAILED'
      + '. Step ' + CONVERT(VARCHAR,ISNULL(@v_step,0))
      + '. Error Num: ' + CONVERT(VARCHAR,ISNULL(ERROR_NUMBER(),0))
      + '. Error Msg: ' + ERROR_MESSAGE(),1,255)

  END CATCH
  IF XACT_STATE() <> 0
  BEGIN
    ROLLBACK TRANSACTION
  END

  RETURN 0

