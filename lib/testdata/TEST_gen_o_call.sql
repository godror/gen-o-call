CREATE OR REPLACE PACKAGE TEST_gen_o_call IS
  FUNCTION simple(p_plsint IN OUT PLS_INTEGER, p_num IN OUT NUMBER, p_vc IN OUT VARCHAR2, p_dt IN OUT DATE) RETURN PLS_INTEGER;

  TYPE rec_t IS RECORD (F_plsint PLS_INTEGER, F_num NUMBER, F_vc VARCHAR2(32767), F_dt DATE);
  FUNCTION rec(p_rec IN OUT rec_t, p_plsint IN OUT PLS_INTEGER, p_num IN OUT NUMBER, p_vc IN OUT VARCHAR2, p_dt IN OUT DATE) RETURN PLS_INTEGER;

  TYPE plsint_t IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  TYPE num_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE vc_t IS TABLE OF VARCHAR2(32767) INDEX BY PLS_INTEGER;
  TYPE dt_T IS TABLE OF DATE INDEX BY PLS_INTEGER;
  FUNCTION tab(p_plsint_tab IN OUT NOCOPY plsint_t, p_num_tab IN OUT NOCOPY num_t, p_vc IN OUT NOCOPY vc_t, p_dt IN OUT NOCOPY dt_t) RETURN plsint_t;

  TYPE rec_tt IS TABLE OF rec_t INDEX BY PLS_INTEGER;
  FUNCTION rec_tab(p_rec_tab IN OUT NOCOPY rec_tt) RETURN PLS_INTEGER;

  TYPE rec_rec_t IS RECORD (F_rec rec_t, F_plsint_tab plsint_t, F_num_tab num_t, F_vc_tab vc_t, F_dt_tab dt_t);
  FUNCTION rec_rec_tab(p_rec_rec IN OUT NOCOPY rec_rec_t) RETURN PLS_INTEGER;

  TYPE rec_rec_tt IS TABLE OF rec_rec_t INDEX BY PLS_INTEGER;
  FUNCTION rec_rec_tab_tab(p_rec_rec_tab_tab IN OUT NOCOPY rec_rec_tt) RETURN plsint_t;
END TEST_gen_o_call;

