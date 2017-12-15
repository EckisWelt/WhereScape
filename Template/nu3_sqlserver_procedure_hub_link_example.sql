CREATE PROCEDURE update_hub_demo
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
    DECLARE @v_source_table sysname
    SET @v_source_table = CASE WHEN @p_task_name = 'hub_demo' THEN 'hub_demo' ELSE @p_task_name END

    SELECT @v_msgtext = 'Load data from ' + @v_source_table + ''
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Insert new records
    --==========================================================================
    SET @v_step = 200

    BEGIN TRANSACTION

      SET @v_sql = N'
      INSERT INTO [TABLEOWNER].[hub_demo] WITH ( TABLOCKX )
      (
             hub_product_key
            ,shop_code
            ,sku
            ,dss_load_datetime
            ,dss_record_source
            ,dss_create_datetime
      )
      SELECT 
             hub_demo.hub_product_key AS hub_product_key 
            ,hub_demo.shop_code AS shop_code 
            ,hub_demo.sku AS sku 
            ,hub_demo.dss_load_datetime AS dss_load_datetime 
            ,hub_demo.dss_record_source AS dss_record_source 
            ,@v_current_datetime AS dss_create_datetime 
      FROM   (
        SELECT *
              ,ROW_NUMBER() OVER (PARTITION BY hub_product_key , shop_code , sku ORDER BY dss_load_datetime ASC AS dss_row_no
        FROM   (
          SELECT *
          FROM   (
            SELECT 
                   stage_hub_demo.hub_product_key AS hub_product_key 
                  ,stage_hub_demo.shop_code AS shop_code 
                  ,stage_hub_demo.sku AS sku 
                  ,stage_hub_demo.dss_load_datetime AS dss_load_datetime 
                  ,stage_hub_demo.dss_record_source AS dss_record_source 
            FROM [TABLEOWNER].[stage_hub_demo] stage_hub_demo
          ) AS hub_demo
          WHERE NOT EXISTS (
                  SELECT 1
                  FROM   [TABLEOWNER].[hub_demo] hub_demo__not_exist
                  WHERE  hub_demo__not_exist.hub_product_key = hub_demo.hub_product_key
                         AND hub_demo__not_exist.shop_code = hub_demo.shop_code
                         AND hub_demo__not_exist.sku = hub_demo.sku
          )
        ) AS hub_demo
      ) AS hub_demo
      WHERE dss_row_no = 1
      '

      SET @v_sql = REPLACE(@v_sql,'@v_current_datetime','CAST(''' + CAST(@v_current_datetime AS NVARCHAR) + ''' AS DATETIME2(7))')
      SET @v_sql = REPLACE(@v_sql,'stage_hub_demo',@v_source_table)

      EXEC (@v_sql)

      SELECT @v_row_count = @@ROWCOUNT

    COMMIT

    SET @v_insert_count = @v_insert_count + @v_row_count    SELECT @v_msgtext = 'hub_demo updated. ' + CONVERT(VARCHAR,@v_row_count) + ' records added.'
    EXEC dbo.WsWrkError 'I',@p_job_name,@p_task_name,@p_sequence,@v_msgtext,NULL,NULL,@p_task_id,@p_job_id,NULL
    
    --==========================================================================
    -- Finish Up
    --==========================================================================
    SET @v_step = 300

    EXEC WsWrkTask @p_job_id, @p_task_id, @p_sequence, @v_insert_count, @v_update_count, 0, @v_delete_count, 0, 0, 0 

    SET @p_status = 1
    SET @p_return_msg = 'hub_demo updated. '
      + CASE WHEN @v_delete_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_delete_count,0)) + ' records deleted. ' ELSE '' END
      + CONVERT(VARCHAR,COALESCE(@v_insert_count,0)) + ' records added. '
      + CASE WHEN @v_update_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_update_count,0)) + ' records updated. ' ELSE '' END
      + CASE WHEN @v_change_count <> 0 THEN CONVERT(VARCHAR,COALESCE(@v_change_count,0)) + ' records changed. ' ELSE '' END

    RETURN 0

  END TRY
  BEGIN CATCH

    SET @p_status = -2
    SET @p_return_msg = SUBSTRING('update_hub_demo updated FAILED'
      + '. Step ' + CONVERT(VARCHAR,ISNULL(@v_step,0))
      + '. Error Num: ' + CONVERT(VARCHAR,ISNULL(ERROR_NUMBER(),0))
      + '. Error Msg: ' + ERROR_MESSAGE(),1,255)

  END CATCH
  IF XACT_STATE() <> 0
  BEGIN
    ROLLBACK TRANSACTION
  END

  RETURN 0
