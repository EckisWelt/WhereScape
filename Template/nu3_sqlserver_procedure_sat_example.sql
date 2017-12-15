CREATE PROCEDURE update_sat_demo
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
    SELECT @v_msgtext = 'Load data from stage_sat_demo'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Insert new records
    --==========================================================================
    SET @v_step = 200

    BEGIN TRANSACTION

      INSERT INTO [TABLEOWNER].[sat_demo] WITH ( TABLOCK )
      (
             hub_product_key
            ,displayable_quantity
            ,dss_event_datetime
            ,dss_load_datetime
            ,dss_record_source
            ,dss_change_hash
            ,dss_start_datetime
            ,dss_version
            ,dss_create_datetime
      )
      SELECT 
             sat_demo.hub_product_key AS hub_product_key 
            ,sat_demo.displayable_quantity AS displayable_quantity 
            ,sat_demo.dss_event_datetime AS dss_event_datetime 
            ,sat_demo.dss_load_datetime AS dss_load_datetime 
            ,sat_demo.dss_record_source AS dss_record_source 
            ,sat_demo.dss_change_hash AS dss_change_hash 
            ,sat_demo.dss_start_datetime AS dss_start_datetime 
            ,sat_demo.dss_version AS dss_version 
            ,@v_current_datetime AS dss_create_datetime 
      FROM   (
        SELECT sat_demo.*
              ,dss_load_datetime AS dss_start_datetime
              ,CASE WHEN current_rows.hub_product_key IS NULL
                    THEN 1
                    ELSE current_rows.dss_version + dss_row_no
               END AS dss_version
        FROM   (
          SELECT sat_demo.*
                ,ROW_NUMBER() OVER ( PARTITION BY hub_product_key ORDER BY dss_load_datetime ) AS dss_row_no
          FROM   (
            SELECT sat_demo.*
                  ,CASE WHEN LAG( dss_change_hash , 1 , CONVERT(BINARY(16),'') ) OVER ( PARTITION BY hub_product_key ORDER BY dss_load_datetime ) = dss_change_hash THEN 0
                        ELSE 1
                   END AS is_hash_change
                  ,CASE WHEN LAG( dss_load_datetime , 1 , '0001-01-01' ) OVER ( PARTITION BY hub_product_key ORDER BY dss_load_datetime ) = dss_load_datetime THEN 0
                        ELSE 1
                   END AS is_load_datetime_change
            FROM   (
              SELECT sat_demo.*
              FROM   (
                SELECT 
                       stage_sat_demo.hub_product_key AS hub_product_key 
                      ,stage_sat_demo.displayable_quantity AS displayable_quantity 
                      ,stage_sat_demo.dss_event_datetime AS dss_event_datetime 
                      ,stage_sat_demo.dss_load_datetime AS dss_load_datetime 
                      ,stage_sat_demo.dss_record_source AS dss_record_source 
                      ,stage_sat_demo.dss_change_hash AS dss_change_hash 
                FROM [TABLEOWNER].[stage_sat_demo] stage_sat_demo
              ) AS sat_demo
              WHERE NOT EXISTS (
                      SELECT 1
                      FROM   [TABLEOWNER].[sat_demo] sat_demo__not_exist
                      WHERE  sat_demo__not_exist.hub_product_key = sat_demo.hub_product_key
                             AND sat_demo__not_exist.dss_load_datetime < sat_demo.dss_load_datetime
              )
            ) AS sat_demo
          ) AS sat_demo
          WHERE  is_hash_change = 1
          AND    is_load_datetime_change = 1
        ) AS sat_demo
        LEFT OUTER JOIN (
                SELECT
                       sat_demo__current.hub_product_key AS hub_product_key
                      ,MAX(sat_demo__current.dss_start_datetime) AS dss_start_datetime
                      ,MAX(sat_demo__current.dss_version) AS dss_version
                FROM   [TABLEOWNER].[sat_demo] sat_demo__current
                GROUP BY
                       sat_demo__current.hub_product_key
        ) AS current_rows
            ON  current_rows.hub_product_key = sat_demo.hub_product_key
      ) AS sat_demo
      ;

      SELECT @v_row_count = @@ROWCOUNT

    COMMIT

    SET @v_insert_count = @v_insert_count + @v_row_count    
    
    SELECT @v_msgtext = 'sat_demo updated. ' + CONVERT(VARCHAR,@v_row_count) + ' records added.'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Finish Up
    --==========================================================================
    SET @v_step = 300

    EXEC WsWrkTask @p_job_id, @p_task_id, @p_sequence, @v_insert_count, @v_update_count, 0, @v_delete_count, 0, 0, 0 

    SET @p_status = 1
    SET @p_return_msg = 'sat_demo updated. '
      + CASE WHEN @v_delete_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_delete_count,0)) + ' records deleted. ' ELSE '' END
      + CONVERT(VARCHAR,COALESCE(@v_insert_count,0)) + ' records added. '
      + CASE WHEN @v_update_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_update_count,0)) + ' records updated. ' ELSE '' END
      + CASE WHEN @v_change_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_change_count,0)) + ' records changed. ' ELSE '' END

    RETURN 0

  END TRY
  BEGIN CATCH

    SET @p_status = -2
    SET @p_return_msg = SUBSTRING('update_sat_demo updated FAILED'
      + '. Step ' + CONVERT(VARCHAR,ISNULL(@v_step,0))
      + '. Error Num: ' + CONVERT(VARCHAR,ISNULL(ERROR_NUMBER(),0))
      + '. Error Msg: ' + ERROR_MESSAGE(),1,255)

  END CATCH
  IF XACT_STATE() <> 0
  BEGIN
    ROLLBACK TRANSACTION
  END

  RETURN 0

