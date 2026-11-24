const {readFileSync}=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const ctx={};vm.createContext(ctx);vm.runInContext(readFileSync(__dirname+'/static/incident-center.js','utf8')+';this.guide=incidentGuide;',ctx);
const snapshot={queue:{counts:{waiting_context:19,quarantined:28}},queries:[{name:'silver_learning_events',label:'Silver 학습',active:false,batch_id:4,input_rows:5,batch_ms:6000}]};
let g=ctx.guide({id:'context-age'},snapshot);assert.equal(g.target,'#state-waiting_context');assert.match(g.evidence,/19/);assert.match(g.action,/UUID/);
g=ctx.guide({id:'slow:silver_learning_events'},snapshot);assert.equal(g.target,'#query-silver_learning_events');assert.match(g.evidence,/6000ms/);
g=ctx.guide({id:'quarantined'},snapshot);assert.equal(g.target,'#state-quarantined');assert.match(g.action,/필수 필드/);
g=ctx.guide({id:'context-age'},{queue:{counts:null}});assert.match(g.evidence,/확인할 수 없습니다/);
console.log('Incident detail checks passed: matching query/state targets, evidence, action guides, unknown data.');
