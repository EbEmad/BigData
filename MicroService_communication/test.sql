DO $$
BEGIN
    IF 10 > 5 THEN
        RAISE NOTICE 'welcome hema';
    ELSE
        RAISE NOTICE 'welcome anas';
    END IF;
END $$;