import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-info-panel',
  templateUrl: './info-panel.component.html',
  styleUrls: ['./info-panel.component.css']
})
export class InfoPanelComponent implements OnInit {
  @Input() title:string = 'TOTAL AREA'
  value:string = '23 456'

  constructor() { }

  ngOnInit(): void {
  }

}
