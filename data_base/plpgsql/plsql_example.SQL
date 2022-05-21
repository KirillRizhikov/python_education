-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
-- Sting constants
select 'String constant';
select 'I''m also a string constant';
select E'I\'m also a string constant';

select $$I'm a string constant that contains a backslash \$$;
SELECT $message$I'm a string constant that contains a backslash \$message$;

do
'declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice ''The number of films: %'', film_count;
end;';


-- Dollar-quoted string constant
do
$$
declare
   film_count integer;
begin
   select count(*) into film_count
   from film;
   raise notice 'The number of films: %', film_count;
end;
$$;


-- Functions
create function find_film_by_id(
   id int
) returns film
language sql
as
  'select * from film
   where film_id = id';

create function find_film_by_id(
   id int
) returns film
language sql
as
$$
  select * from film
  where film_id = id;
$$;
drop function find_film_by_id(id int);


-- Single block structure
do $$
<<first_block>>
declare
  film_count integer := 0;
begin
   -- get the number of films
   select count(*)
   into film_count
   from film;
   -- display a message
   raise notice 'The number of films is %', film_count;
end first_block $$;


-- Variables
do $$
declare
   counter    integer := 1;
   first_name varchar(50) := 'John';
   last_name  varchar(50) := 'Doe';
   payment    numeric(11,2) := 20.5;
begin
   raise notice '% % % has been paid % USD',
       counter,
	   first_name,
	   last_name,
	   payment;
end $$;

do $$
declare
   created_at time := now();
begin
   raise notice '%', created_at;
   perform pg_sleep(10);
   raise notice '%', created_at;
end $$;

do $$
declare
   film_title film.title%type;
   featured_title film_title%type;
begin
   -- get title of the film id 100
   select title
   from film
   where film_id = 100
   into film_title;

   -- show the film title
   raise notice 'Film title id 100: %s', film_title;
end; $$;


-- Subblocks
do $$
<<outer_block>>
declare
  counter integer := 0;
begin
   counter := counter + 1;
   raise notice 'The current value of the counter is %', counter;

   declare
       counter integer := 0;
   begin
       counter := counter + 10;
       raise notice 'Counter in the subblock is %', counter;
       raise notice 'Counter in the outer block is %', outer_block.counter;
   end;

   raise notice 'Counter in the outer block is %', counter;

end outer_block $$;


-- Select into
do $$
declare
   actor_count integer;
begin
   -- select the number of actors from the actor table
   select count(*)
   into actor_count
   from actor;

   -- show the number of actors
   raise notice 'The number of actors: %', actor_count;
end; $$;


-- Row type usage
do $$
declare
   selected_actor actor%rowtype;
begin
   -- select actor with id 10
   select *
   from actor
   into selected_actor
   where actor_id = 10;

   -- show the number of actor
   raise notice 'The actor name is % %',
      selected_actor.first_name,
      selected_actor.last_name;
end; $$;

do
$$
declare
	rec record;
begin
	-- select the film
	select film_id, title, length
	into rec
	from film
	where film_id = 200;

	raise notice '% % %', rec.film_id, rec.title, rec.length;

end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	for rec in select title, length
			from film
			where length > 50
			order by length
	loop
		raise notice '% (%)', rec.title, rec.length;
	end loop;
end;
$$;


-- Constants
do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat );
end $$;

do $$
declare
   vat constant numeric := 0.1;
   net_price    numeric := 20.5;
begin
   raise notice 'The selling price is %', net_price * ( 1 + vat);
   vat := 0.05;
end $$;

do $$
declare
   start_at constant time := now();
begin
   raise notice 'Start executing block at %', start_at;
end $$;


-- Errors
do $$
begin
  raise info 'information message %', now() ;
  raise log 'log message %', now();
  raise debug 'debug message %', now();
  raise warning 'warning message %', now();
  raise notice 'notice message %', now();
end $$;

do $$
declare
  email varchar(255) := 'info@postgresqltutorial.com';
begin
  -- check email for duplicate
  -- ...
  -- report duplicate email
  raise exception 'duplicate email: %', email
		using hint = 'check the email again';
end $$;

do $$
begin
	--...
	raise sqlstate '2210b';
end $$;

do $$
begin
	--...
	raise invalid_regular_expression;
end $$;


-- Assertions
do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 0, 'Film not found, check the film table';
end$$;

do $$
declare
   film_count integer;
begin
   select count(*)
   into film_count
   from film;

   assert film_count > 1000, '1000 Film found, check the film table';
end$$;


-- Exceptions handling
do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_film_id int = 2000;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where film_id = v_film_id;
        -- catch exception
	exception
	   when no_data_found then
	      raise exception 'film % not found', v_film_id;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
begin
	-- select film
	select film_id, title
	into strict rec
	from film
	where title LIKE 'A%';

	exception
	   when too_many_rows then
	      raise exception 'Search query returns too many rows';
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 90;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The film with length % is not unique', v_length;
end;
$$
language plpgsql;

do
$$
declare
	rec record;
	v_length int = 30;
begin
	-- select a film
	select film_id, title
	into strict rec
	from film
	where length = v_length;

        -- catch exception
	exception
	   when sqlstate 'P0002' then
	      raise exception 'film with length % not found', v_length;
	   when sqlstate 'P0003' then
	      raise exception 'The with length % is not unique', v_length;
end;
$$
language plpgsql;


-- If
do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 0;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice'The film % could not be found',
	    input_film_id;
  end if;
end $$;

do $$
declare
  selected_film film%rowtype;
  input_film_id film.film_id%type := 100;
begin

  select * from film
  into selected_film
  where film_id = input_film_id;

  if not found then
     raise notice 'The film % could not be found',
	    input_film_id;
  else
     raise notice 'The film title is %', selected_film.title;
  end if;
end $$;

do $$
declare
   v_film film%rowtype;
   len_description varchar(100);
begin

  select * from film
  into v_film
  where film_id = 100;

  if not found then
     raise notice 'Film not found';
  else
      if v_film.length >0 and v_film.length <= 50 then
		 len_description := 'Short';
	  elsif v_film.length > 50 and v_film.length < 120 then
		 len_description := 'Medium';
	  elsif v_film.length > 120 then
		 len_description := 'Long';
	  else
		 len_description := 'N/A';
	  end if;

	  raise notice 'The % film is %.',
	     v_film.title,
	     len_description;
  end if;
end $$;


-- Case
do $$
declare
	rate   film.rental_rate%type;
	price_segment varchar(50);
begin
    -- get the rental rate
    select rental_rate into rate
    from film
    where film_id = 100;

	-- assign the price segment
	if found then
		case rate
		   when 0.99 then
              price_segment =  'Mass';
		   when 2.99 then
              price_segment = 'Mainstream';
		   when 4.99 then
              price_segment = 'High End';
		   else
	    	  price_segment = 'Unspecified';
		   end case;
		raise notice '%', price_segment;
    end if;
end; $$;

do $$
declare
    total_payment numeric;
    service_level varchar(25) ;
begin
     select sum(amount) into total_payment
     from Payment
     where customer_id = 100;

	 if found then
	    case
		   when total_payment > 200 then
               service_level = 'Platinum' ;
           when total_payment > 100 then
	           service_level = 'Gold' ;
           else
               service_level = 'Silver' ;
        end case;
		raise notice 'Service Level: %', service_level;
     else
	    raise notice 'Customer not found';
	 end if;
end; $$;


-- Loop
do $$
declare
   n integer:= 10;
   fib integer := 0;
   counter integer := 0 ;
   i integer := 0 ;
   j integer := 1 ;
begin
	if (n < 1) then
		fib := 0 ;
	end if;
	loop
		exit when counter = n ;
		counter := counter + 1 ;
		select j, i + j into i,	j ;
	end loop;
	fib := i;
    raise notice '%', fib;
end; $$;

-- While
do $$
declare
   counter integer := 0;
begin
   while counter < 5 loop
      raise notice 'Counter %', counter;
	  counter := counter + 1;
   end loop;
end$$;


-- For loop
do $$
begin
   for counter in 1..5 loop
	raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
   for counter in reverse 5..1 loop
      raise notice 'counter: %', counter;
   end loop;
end; $$;

do $$
begin
  for counter in 1..6 by 2 loop
    raise notice 'counter: %', counter;
  end loop;
end; $$;


-- Iterate over results
do
$$
declare
    f record;
begin
    for f in select title, length
	       from film
	       order by length desc, title
	       limit 10
    loop
	raise notice '% (% mins)', f.title, f.length;
    end loop;
end;
$$;

do $$
declare
    -- sort by 1: title, 2: release year
    sort_type smallint := 1;
	-- return the number of films
	rec_count int := 10;
	-- use to iterate over the film
	rec record;
	-- dynamic query
    query text;
begin

	query := 'select title, release_year from film ';

	if sort_type = 1 then
		query := query || 'order by title';
	elsif sort_type = 2 then
	  query := query || 'order by release_year';
	else
	   raise 'invalid sort type %s', sort_type;
	end if;

	query := query || ' limit $1';

	for rec in execute query using rec_count
        loop
	     raise notice '% - %', rec.release_year, rec.title;
	end loop;
end;
$$;

-- Exit from loop
do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
declare
   i int = 0;
   j int = 0;
begin
  <<outer_loop>>
  loop
     i = i + 1;
     exit when i > 3;
	 -- inner loop
	 j = 0;
     <<inner_loop>>
     loop
		j = j + 1;
		exit outer_loop when j > 3;
		raise notice '(i,j): (%,%)', i, j;
	 end loop inner_loop;
  end loop outer_loop;
end;
$$;

do
$$
begin

  <<simple_block>>
   begin
  	 exit simple_block;
         -- for demo purposes
	 raise notice '%', 'unreachable!';
   end;
   raise notice '%', 'End of block';
end;
$$;

-- Continue iterations
do
$$
declare
   counter int = 0;
begin

  loop
     counter = counter + 1;
	 -- exit the loop if counter > 10
	 exit when counter > 10;
	 -- skip the current iteration if counter is an even number
	 continue when mod(counter,2) = 0;
	 -- print out the counter
	 raise notice '%', counter;
  end loop;
end;
$$;
