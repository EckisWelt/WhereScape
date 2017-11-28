CREATE PROCEDURE update_stage_sat_demo
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
  -- MAIN
  --============================================================================
  SET @v_step = 100

  SET @v_insert_count = 0
  SET @v_current_datetime = SYSDATETIME()

  BEGIN TRY

    --==========================================================================
    -- Source
    --==========================================================================
    SELECT @v_msgtext = 'Load data from source_demo'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Delete existing records
    --==========================================================================
    SET @v_step = 200

    SET @v_sql = N'TRUNCATE TABLE [TABLEOWNER].[stage_sat_demo];'
    EXEC @v_return_status = sp_executesql @v_sql

    --==========================================================================
    -- Temporary Table
    --==========================================================================
    SET @v_step = 300

    SET @v_row_count = 0

    IF OBJECT_ID('tempdb..#stage_sat_demo') IS NOT NULL
      DROP TABLE #stage_sat_demo

    SELECT 
           source_demo.shop_code AS shop_code 
          ,source_demo.sid AS session_id 
          ,source_demo.request_id AS request_id 
          ,source_demo.times AS times 
          ,source_demo.referrer AS referrer 
          ,source_demo.url AS url 
          ,source_demo.search_engine AS search_engine 
          ,DATEADD(NS,100*dss_sequence_no,dss_load_datetime) AS dss_load_datetime 
          ,source_demo.dss_record_source AS dss_record_source 
    INTO   #stage_sat_demo
    FROM   [TABLEOWNER].[source_demo]
    WHERE  dss_load_datetime > ( 
    SELECT ISNULL(MAX(t.dss_load_datetime) , '0001-01-01')
    FROM   [TABLEOWNER].[sat_demo] AS t
    )
    ;

    SELECT @v_row_count = @@ROWCOUNT
    SELECT @v_msgtext = '#stage_sat_demo updated. ' + CONVERT(VARCHAR,@v_row_count) + ' records added.'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Insert new records
    --==========================================================================
    SET @v_step = 400

    BEGIN TRANSACTION

      INSERT INTO [TABLEOWNER].[stage_sat_demo] WITH ( TABLOCKX )
      (
             hub_request
            ,shop_code
            ,session_id
            ,request_id
            ,times
            ,referrer
            ,url
            ,search_engine
            ,dss_load_datetime
            ,dss_record_source
            ,dss_change_hash
            ,dss_create_datetime
            ,dss_update_datetime
      )
      SELECT 
             stage_sat_demo.hub_request AS hub_request 
            ,stage_sat_demo.shop_code AS shop_code 
            ,stage_sat_demo.session_id AS session_id 
            ,stage_sat_demo.request_id AS request_id 
            ,stage_sat_demo.times AS times 
            ,stage_sat_demo.referrer AS referrer 
            ,stage_sat_demo.url AS url 
            ,stage_sat_demo.search_engine AS search_engine 
            ,stage_sat_demo.dss_load_datetime AS dss_load_datetime 
            ,stage_sat_demo.dss_record_source AS dss_record_source 
            ,stage_sat_demo.dss_change_hash AS dss_change_hash 
            ,@v_current_datetime AS dss_create_datetime 
            ,@v_current_datetime AS dss_update_datetime 
      FROM   (
        SELECT stage_sat_demo.*
        FROM   (
          SELECT *
                ,ROW_NUMBER() OVER (PARTITION BY hub_request , shop_code , session_id ORDER BY dss_load_datetime DESC) AS dss_row_no
          FROM   (
            SELECT *
                  ,HASHBYTES('md5',
                      CAST(shop_code AS NVARCHAR(MAX)) +'||'+
                      CAST(session_id AS NVARCHAR(MAX)) +'||'+
                      CAST(request_id AS NVARCHAR(MAX))
                      ) AS hub_request
                  ,HASHBYTES('md5',
                      COALESCE(CAST(referrer AS NVARCHAR(MAX)),'') +'||'+
                      COALESCE(CAST(search_engine AS NVARCHAR(MAX)),'') +'||'+
                      COALESCE(CAST(times AS NVARCHAR(MAX)),'') +'||'+
                      COALESCE(CAST(url AS NVARCHAR(MAX)),'')
                      ) AS dss_change_hash
            FROM   (
              SELECT 
                     UPPER(LTRIM(RTRIM(COALESCE(NULLIF(shop_code,''),'-1')))) AS shop_code
                    ,COALESCE(session_id,-1) AS session_id
                    ,COALESCE(request_id,-1) AS request_id
                    ,times AS times
                    ,referrer AS referrer
                    ,url AS url
                    ,search_engine AS search_engine
                    ,dss_load_datetime AS dss_load_datetime
                    ,dss_record_source AS dss_record_source
              FROM   (
                SELECT *
                FROM   #stage_sat_demo AS stage_sat_demo
              ) AS stage_sat_demo
            ) AS stage_sat_demo
          ) AS stage_sat_demo
        ) AS stage_sat_demo
        WHERE template_stage_sat.dss_row_no = 1
      ) AS stage_sat_demo
      ;

      SELECT @v_row_count = @@ROWCOUNT

    COMMIT

    SET @v_insert_count = @v_insert_count + @v_row_count
    SELECT @v_msgtext = 'stage_sat_demo updated. ' + CONVERT(VARCHAR,@v_row_count) + ' records added.'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Clean Up
    --==========================================================================
    SET @v_step = 500

    IF OBJECT_ID('tempdb..#stage_sat_demo') IS NOT NULL
      DROP TABLE #stage_sat_demo

    --==========================================================================
    -- Finish Up
    --==========================================================================
    SET @v_step = 600

    EXEC WsWrkTask @p_job_id, @p_task_id, @p_sequence, @v_insert_count, @v_update_count, 0, @v_delete_count, 0, 0, 0 

    SET @p_status = 1
    SET @p_return_msg = 'stage_sat_demo updated. '
      + CASE WHEN @v_delete_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_delete_count,0)) + ' records deleted. ' ELSE '' END
      + CONVERT(VARCHAR,COALESCE(@v_insert_count,0)) + ' records added. '
      + CASE WHEN @v_update_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_update_count,0)) + ' records updated. ' ELSE '' END
      + CASE WHEN @v_change_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_change_count,0)) + ' records changed. ' ELSE '' END

    RETURN 0

  END TRY
  BEGIN CATCH

    SET @p_status = -2
    SET @p_return_msg = SUBSTRING('update_stage_sat_demo updated FAILED'
      + '. Step ' + CONVERT(VARCHAR,ISNULL(@v_step,0))
      + '. Error Num: ' + CONVERT(VARCHAR,ISNULL(ERROR_NUMBER(),0))
      + '. Error Msg: ' + ERROR_MESSAGE(),1,255)

  END CATCH
  IF XACT_STATE() <> 0
  BEGIN
    ROLLBACK TRANSACTION
  END

  RETURN 0
