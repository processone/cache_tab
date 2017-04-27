/*
 * Author: Evgeny Khramtsov <ekhramtsov@process-one.net>
 * Created : 26 Apr 2017 by Evgeny Khramtsov <ekhramtsov@process-one.net>
 *
 *
 * Copyright (C) 2002-2017 ProcessOne, SARL. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <erl_nif.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#define MAX_UINT64 ((ErlNifUInt64) -1)

static unsigned int max_counters = 0;
static unsigned int counters_num = 0;
static ErlNifUInt64 *counters = NULL;
static ErlNifMutex *counters_lock = NULL;

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM max)
{
  size_t size;
  if (enif_get_uint(env, max, &max_counters)) {
    size = sizeof(ErlNifUInt64) * max_counters;
    if ((counters = enif_alloc(size))) {
      memset(counters, -1, size);
      counters_num = 0;
      counters_lock = enif_mutex_create("counters_lock");
      return 0;
    }
  }

  return ENOMEM;
}

static void unload(ErlNifEnv* env, void* priv)
{
  if (counters_lock) enif_mutex_destroy(counters_lock);
  if (counters) enif_free(counters);
}

static ERL_NIF_TERM new_counter(ErlNifEnv* env, int argc,
				const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  enif_mutex_lock(counters_lock);
  if (counters_num < max_counters) {
    counter = counters_num++;
    counters[counter] = 0;
    enif_mutex_unlock(counters_lock);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"),
			    enif_make_ulong(env, counter));
  } else {
    for (counter = 0; counter < max_counters; counter++) {
      if (counters[counter] == MAX_UINT64) {
	counters[counter] = 0;
	enif_mutex_unlock(counters_lock);
	return enif_make_tuple2(env, enif_make_atom(env, "ok"),
				enif_make_ulong(env, counter));
      }
    }
    enif_mutex_unlock(counters_lock);
    return enif_make_tuple2(env, enif_make_atom(env, "error"),
			    enif_make_tuple2(env, enif_make_atom(env, "system_limit"),
					     enif_make_uint(env, max_counters)));
  }
}

static ERL_NIF_TERM delete_counter(ErlNifEnv* env, int argc,
				   const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  if (enif_get_uint(env, argv[0], &counter))
    if (counter < max_counters) {
      counters[counter] = MAX_UINT64;
      return enif_make_atom(env, "ok");
    }

  return enif_make_badarg(env);
}

static ERL_NIF_TERM incr_counter(ErlNifEnv* env, int argc,
				 const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  if (enif_get_uint(env, argv[0], &counter))
    if (counter < max_counters) {
      if (counters[counter] != MAX_UINT64)
	return enif_make_uint64(env, __sync_add_and_fetch(counters + counter, 1));
    }

  return enif_make_badarg(env);
}

static ERL_NIF_TERM get_counter(ErlNifEnv* env, int argc,
				const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  ErlNifUInt64 val;
  if (enif_get_uint(env, argv[0], &counter))
    if (counter < max_counters) {
      val = counters[counter];
      if (val != MAX_UINT64)
	return enif_make_uint64(env, val);
    }

  return enif_make_badarg(env);
}

static ErlNifFunc nif_funcs[] =
    {
      {"new_counter_nif", 0, new_counter},
      {"delete_counter_nif", 1, delete_counter},
      {"incr_counter_nif", 1, incr_counter},
      {"get_counter_nif", 1, get_counter}
    };

ERL_NIF_INIT(ets_cache, nif_funcs, load, NULL, NULL, unload)
