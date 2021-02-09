/*
 * Author: Evgeny Khramtsov <ekhramtsov@process-one.net>
 * Created : 26 Apr 2017 by Evgeny Khramtsov <ekhramtsov@process-one.net>
 *
 *
 * Copyright (C) 2002-2021 ProcessOne, SARL. All Rights Reserved.
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

typedef struct State {
  unsigned int max_counters;
  unsigned int counters_num;
  ErlNifUInt64 *counters;
  ErlNifMutex *counters_lock;
} State;

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM max)
{
  size_t size;
  unsigned int max_counters;

  State *state;

  if (!(state = enif_alloc(sizeof(*state)))) {
      return ENOMEM;
  }

  if (enif_get_uint(env, max, &max_counters)) {
    size = sizeof(ErlNifUInt64) * max_counters;
    if ((state->counters = enif_alloc(size))) {
      memset(state->counters, -1, size);
      state->counters_num = 0;
      state->counters_lock = enif_mutex_create("counters_lock");
      *priv = state;
      return 0;
    } else {
      enif_free(state);
    }
  }

  return ENOMEM;
}

static void unload(ErlNifEnv* env, void* priv)
{
  if (!priv)
    return;

  State *state = (State*)priv;

  if (state->counters_lock) enif_mutex_destroy(state->counters_lock);
  if (state->counters) enif_free(state->counters);

  enif_free(state);
}

static int upgrade(ErlNifEnv* caller_env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  if (!*old_priv_data) {
    return 1;
  }

  *priv_data = *old_priv_data;
  *old_priv_data = NULL;

  return 0;
}

static ERL_NIF_TERM new_counter(ErlNifEnv* env, int argc,
				const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  State *state = (State*)enif_priv_data(env);
  enif_mutex_lock(state->counters_lock);

  if (state->counters_num < state->max_counters) {
    counter = state->counters_num++;
    state->counters[counter] = 0;
    enif_mutex_unlock(state->counters_lock);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"),
			    enif_make_ulong(env, counter));
  } else {
    for (counter = 0; counter < state->max_counters; counter++) {
      if (state->counters[counter] == MAX_UINT64) {
        state->counters[counter] = 0;
        enif_mutex_unlock(state->counters_lock);
        return enif_make_tuple2(env, enif_make_atom(env, "ok"),
				enif_make_ulong(env, counter));
      }
    }
    enif_mutex_unlock(state->counters_lock);
    return enif_make_tuple2(env, enif_make_atom(env, "error"),
                            enif_make_tuple2(env, enif_make_atom(env, "system_limit"),
                                             enif_make_uint(env, state->max_counters)));
  }
}

static ERL_NIF_TERM delete_counter(ErlNifEnv* env, int argc,
				   const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  State *state = (State*)enif_priv_data(env);

  if (enif_get_uint(env, argv[0], &counter))
    if (counter < state->max_counters) {
      state->counters[counter] = MAX_UINT64;
      return enif_make_atom(env, "ok");
    }

  return enif_make_badarg(env);
}

static ERL_NIF_TERM incr_counter(ErlNifEnv* env, int argc,
				 const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  State *state = (State*)enif_priv_data(env);

  if (enif_get_uint(env, argv[0], &counter))
    if (counter < state->max_counters) {
      if (state->counters[counter] != MAX_UINT64)
        return enif_make_uint64(env, __sync_add_and_fetch(state->counters + counter, 1));
    }

  return enif_make_badarg(env);
}

static ERL_NIF_TERM get_counter(ErlNifEnv* env, int argc,
				const ERL_NIF_TERM argv[])
{
  unsigned int counter;
  ErlNifUInt64 val;
  State *state = (State*)enif_priv_data(env);

  if (enif_get_uint(env, argv[0], &counter))
    if (counter < state->max_counters) {
      val = state->counters[counter];
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

ERL_NIF_INIT(ets_cache, nif_funcs, load, NULL, upgrade, unload)
